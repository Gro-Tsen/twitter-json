#! /usr/local/bin/perl -w

use strict;
use warnings;

use Getopt::Std;

use JSON::XS;
use DateTime::Format::Strptime;

use DBI qw(:sql_types);
use DBD::Pg qw(:pg_types);

my %opts;
getopts('hws:', \%opts);

my $har_mode = $opts{h};

my $global_weak = $opts{w};
my $global_source = $opts{s} // "json-feed";

my $json_coder_unicode = JSON::XS->new;
my $json_decoder_unicode = JSON::XS->new;
my $json_coder_utf8 = JSON::XS->new->utf8;
my $json_decoder_utf8 = JSON::XS->new->utf8;

# binmode STDOUT, ":utf8";

sub html_quote {
    my $str = shift;
    $str =~ s/\&/\&amp;/g;
    $str =~ s/\</\&lt;/g;
    $str =~ s/\>/\&gt;/g;
    $str =~ s/\"/\&quot;/g;
    return $str;
}

sub html_unquote {
    my $str = shift;
    die "Unknown XML entity" if $str =~ m/\&(?!apos\;|quot\;|gt\;|lt\;|amp\;)/;
    $str =~ s/\&apos;/\'/g;
    $str =~ s/\&quot;/\"/g;
    $str =~ s/\&gt;/\>/g;
    $str =~ s/\&lt;/\</g;
    $str =~ s/\&amp;/\&/g;
    return $str;
}

# The following function performs a set of substitutions on a string
# (correctly adjusting the position of each subsequent substitution to
# take into account earlier ones).
sub substitute_in_string {
    my $str = shift;
    my @subs = sort { $a->[0] <=> $b->[0] } @{ shift() };
    # Each entry of @subs is an array reference:
    # [0]: first character of substring to modify
    # [1]: length of substring to modify
    # [2]: undefined, or substring itself, to check for consistency
    # [3]: undefined, or substring to substitute
    # [4]: undefined, or substring to insert before
    # [5]: undefined, or substring to insert at end
    my $corr = 0;
    my $minbar = 0;
    for my $sb ( @subs ) {
	my $idx0 = $sb->[0] + $corr;
	unless ( $idx0>=$minbar ) {
	    print STDERR "substitute_in_string: attempting to overlap substitutions\n";
	    next;
	}
	my $len = $sb->[1];
	if ( defined($sb->[2]) ) {
	    print STDERR (sprintf("substitute_in_string: verification failed: expecting \"%s\", got \"%s\"\n", $sb->[2], substr($str, $idx0, $len))) unless substr($str, $idx0, $len) eq $sb->[2];
	}
	my $repl = ($sb->[4] // "") . ($sb->[3] // substr($str, $idx0, $len)) . ($sb->[5] // "");
	my $newlen = length($repl);
	# printf STDERR "str: %s\nsubstituting (%d,%d) \"%s\"->\"%s\"\n", $str, $idx0, $len, substr($str, $idx0, $len), $repl;
	substr($str, $idx0, $len) = $repl;
	# printf STDERR "str: %s\n", $str;
	$corr += ($newlen - $len);
	$minbar = $idx0 + $newlen;
    }
    return $str;
}

my $datetime_parser = DateTime::Format::Strptime->new(
    pattern => "%a %b %d %T %z %Y", time_zone => "UTC", locale => "C");

my $dbname = "twitter";
my $dbh;  # Connection to database
my $insert_authority_sth;
my $weak_insert_authority_sth;
my $insert_tweet_sth;
my $weak_insert_tweet_sth;
my $insert_media_sth;
my $weak_insert_media_sth;
my $insert_user_sth;
my $weak_insert_user_sth;

sub do_connect {
    # Connect to database and prepare insert statements.
    $dbh = DBI->connect("dbi:Pg:dbname=$dbname", "", "", {AutoCommit=>1,RaiseError=>1,pg_enable_utf8=>1});
    die ("Can't connect to database: " . $DBI::errstr . "\n") unless $dbh;
    $dbh->do("SET TIME ZONE 0");
    die ("Can't set timezone: " . $dbh->errstr . "\n") if $dbh->err;
    # Prepare command to insert into "authority" table:
    my $command = "INSERT INTO authority "
	. "( id , orig , meta_updated_at , meta_inserted_at , meta_source ) "
	. "VALUES ( ?,?::json,?,?,? ) ";
    my $conflict = "ON CONFLICT ON CONSTRAINT authority_meta_source_id_key DO UPDATE SET "
	. "id = EXCLUDED.id "
	. ", orig = EXCLUDED.orig "
	. ", meta_updated_at = EXCLUDED.meta_updated_at "
	. ", meta_source = EXCLUDED.meta_source ";
    my $noconflict = "ON CONFLICT ON CONSTRAINT authority_meta_source_id_key DO NOTHING ";
    my $returning = "RETURNING id , meta_updated_at";
    $insert_authority_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_authority_sth = $dbh->prepare($command . $noconflict . $returning);
    # Prepare command to insert into "tweets" table:
    $command = "INSERT INTO tweets "
	. "( id , created_at , author_id , author_screen_name "
	. ", conversation_id , thread_id "
	. ", replyto_id , replyto_author_id , replyto_author_screen_name "
	. ", retweeted_id , retweeted_author_id , retweeted_author_screen_name "
	. ", quoted_id , quoted_author_id , quoted_author_screen_name "
	. ", full_text , input_text , lang "
	. ", favorite_count , retweet_count , quote_count , reply_count "
	. ", meta_source ) "
	. "VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ) ";
    $conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "id = EXCLUDED.id "
	. ", created_at = EXCLUDED.created_at "
	. ", author_id = EXCLUDED.author_id "
	. ", author_screen_name = EXCLUDED.author_screen_name "
	. ", conversation_id = COALESCE(EXCLUDED.conversation_id, tweets.conversation_id) "
	. ", thread_id = COALESCE(EXCLUDED.thread_id, tweets.thread_id) "
	. ", replyto_id = COALESCE(EXCLUDED.replyto_id, tweets.replyto_id) "
	. ", replyto_author_id = COALESCE(EXCLUDED.replyto_author_id, tweets.replyto_author_id) "
	. ", replyto_author_screen_name = COALESCE(EXCLUDED.replyto_author_screen_name, tweets.replyto_author_screen_name) "
	. ", retweeted_id = COALESCE(EXCLUDED.retweeted_id, tweets.retweeted_id) "
	. ", retweeted_author_id = COALESCE(EXCLUDED.retweeted_author_id, tweets.retweeted_author_id) "
	. ", retweeted_author_screen_name = COALESCE(EXCLUDED.retweeted_author_screen_name, tweets.retweeted_author_screen_name) "
	. ", quoted_id = COALESCE(EXCLUDED.quoted_id, tweets.quoted_id) "
	. ", quoted_author_id = COALESCE(EXCLUDED.quoted_author_id, tweets.quoted_author_id) "
	. ", quoted_author_screen_name = COALESCE(EXCLUDED.quoted_author_screen_name, tweets.quoted_author_screen_name) "
	. ", full_text = EXCLUDED.full_text "
	. ", input_text = EXCLUDED.input_text "
	. ", lang = COALESCE(EXCLUDED.lang, tweets.lang) "
	. ", favorite_count = COALESCE(EXCLUDED.favorite_count, tweets.favorite_count) "
	. ", retweet_count = COALESCE(EXCLUDED.retweet_count, tweets.retweet_count) "
	. ", quote_count = COALESCE(EXCLUDED.quote_count, tweets.quote_count) "
	. ", reply_count = COALESCE(EXCLUDED.reply_count, tweets.reply_count) "
	. ", meta_updated_at = EXCLUDED.meta_updated_at "
	. ", meta_source = EXCLUDED.meta_source ";
    $noconflict = "ON CONFLICT ( id ) DO NOTHING ";
    $returning = "RETURNING id , meta_updated_at";
    $insert_tweet_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_tweet_sth = $dbh->prepare($command . $noconflict . $returning);
    # Prepare command to insert into "media" table:
    $command = "INSERT INTO media "
	. "( id , parent_id , short_url , display_url "
	. ", media_url , media_type , alt_text "
	. ", meta_source ) "
	. "VALUES ( ?,?,?,?,?,?,?,? ) ";
    $conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "id = EXCLUDED.id "
	. ", parent_id = EXCLUDED.parent_id "
	. ", short_url = EXCLUDED.short_url "
	. ", display_url = EXCLUDED.display_url "
	. ", media_url = COALESCE(EXCLUDED.media_url, media.media_url) "
	. ", media_type = EXCLUDED.media_type "
	. ", alt_text = COALESCE(EXCLUDED.alt_text, media.alt_text) "
	. ", meta_updated_at = EXCLUDED.meta_updated_at "
	. ", meta_source = EXCLUDED.meta_source ";
    $insert_media_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_media_sth = $dbh->prepare($command . $noconflict . $returning);
    # Prepare command to insert into "users" table:
    $command = "INSERT INTO users "
	. "( id , created_at , screen_name , full_name "
	. ", profile_description , profile_input_description , profile_url "
	. ", pinned_id , followers_count , following_count , statuses_count "
	. ", meta_source ) "
	. "VALUES ( ?,?,?,?,?,?,?,?,?,?,?,? ) ";
    $conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "id = EXCLUDED.id "
	. ", created_at = EXCLUDED.created_at "
	. ", screen_name = EXCLUDED.screen_name "
	. ", full_name = COALESCE(EXCLUDED.full_name, users.full_name) "
	. ", profile_description = COALESCE(EXCLUDED.profile_description, users.profile_description) "
	. ", profile_input_description = COALESCE(EXCLUDED.profile_input_description, users.profile_input_description) "
	. ", profile_url = COALESCE(EXCLUDED.profile_url, users.profile_url) "
	. ", pinned_id = COALESCE(EXCLUDED.pinned_id, users.pinned_id) "
	. ", followers_count = COALESCE(EXCLUDED.followers_count, users.followers_count) "
	. ", following_count = COALESCE(EXCLUDED.following_count, users.following_count) "
	. ", statuses_count = COALESCE(EXCLUDED.statuses_count, users.statuses_count) "
	. ", meta_updated_at = EXCLUDED.meta_updated_at "
	. ", meta_source = EXCLUDED.meta_source ";
    $insert_user_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_user_sth = $dbh->prepare($command . $noconflict . $returning);
}

do_connect;

sub record_tweet {
    # Insert tweet into database.  Arguments are the ref to the
    # tweet's (decoded) JSON, and a weak parameter indicating whether
    # we should leave existing entries.
    my $r = shift;
    my $weak = shift || $global_weak;  # If 1 leave existing records be
    my $orig = $json_coder_unicode->encode($r);
    ## Basic stuff
    my $id = $r->{"rest_id"};
    unless ( defined($id) ) {
	print STDERR "tweet has no id: aborting\n";
	return;
    }
    unless ( $id =~ m/\A[0-9]+\z/ ) {
	print STDERR "tweet has badly formed id: aborting\n";
	return;
    }
    my $rl = $r->{"legacy"};
    unless ( defined($rl) && ref($rl) eq "HASH" ) {
	print STDERR "tweet $id has no legacy field: aborting\n";
	return;
    }
    unless ( defined($rl->{"id_str"}) && ($rl->{"id_str"} eq $id) ) {
	print STDERR "tweet $id does not have the same id in legacy field: aborting\n";
	return;
    }
    my $created_at_str = $rl->{"created_at"};
    unless ( defined($created_at_str) ) {
	print STDERR "tweet $id has no creation date: aborting\n";
	return;
    }
    my $created_at = $datetime_parser->parse_datetime($created_at_str);
    unless ( defined($created_at) ) {
	print STDERR "tweet $id has invalid creation date: aborting\n";
	return;
    }
    my $created_at_iso = $created_at->strftime("%Y-%m-%d %H:%M:%S+00:00");
    my $author_id = $rl->{"user_id_str"};
    unless ( defined($author_id) ) {
	print STDERR "tweet $id has no author id: aborting\n";
	return;
    }
    unless ( defined($r->{"core"}->{"user_results"}->{"result"})
	     && defined($r->{"core"}->{"user_results"}->{"result"}->{"__typename"})
	     && ($r->{"core"}->{"user_results"}->{"result"}->{"__typename"} eq "User")
	     && defined($r->{"core"}->{"user_results"}->{"result"}->{"rest_id"})
	     && ($r->{"core"}->{"user_results"}->{"result"}->{"rest_id"} eq $author_id) ) {
	print STDERR "tweet $id has bad or missing author object: aborting\n";
	return;
    }
    my $author_r = $r->{"core"}->{"user_results"}->{"result"};
    my $author_screen_name = $author_r->{"legacy"}->{"screen_name"};
    unless ( defined($author_screen_name) ) {
	print STDERR "tweet $id has no author screen name: aborting\n";
	return;
    }
    my $conversation_id = $rl->{"conversation_id_str"};
    my $thread_id = $rl->{"self_thread"}->{"id_str"};
    ## Replyto
    my ($replyto_id, $replyto_author_id, $replyto_author_screen_name);
    $replyto_id = $rl->{"in_reply_to_status_id_str"};
    if ( defined($replyto_id) ) {
	$replyto_author_id = $rl->{"in_reply_to_user_id_str"};
	$replyto_author_screen_name = $rl->{"in_reply_to_screen_name"};
	# This is fairly frequent (replies to a deleted tweet):
	# unless ( defined($replyto_author_id) && defined($replyto_author_screen_name) ) {
	#     print STDERR "tweet $id replying to $replyto_id does not give replyto author id or screen name\n";
	# }
    }
    ## Retweeted
    my $retweeted_r;
    my ($retweeted_id, $retweeted_author_id, $retweeted_author_screen_name);
    if ( defined($rl->{"retweeted_status_result"}) ) {RETWEETED_IF:{
	my $rtwd = $rl->{"retweeted_status_result"}->{"result"};
	unless ( defined($rtwd)
		 && defined($rtwd->{"__typename"})
		 && ($rtwd->{"__typename"} eq "Tweet")
		 && defined($rtwd->{"rest_id"}) ) {
	    print STDERR "tweet $id retweeting another does not give retweeted id\n";
	    last RETWEETED_IF;
	}
	$retweeted_r = $rtwd;
	$retweeted_id = $rtwd->{"rest_id"};
	my $rtwdl = $rtwd->{"legacy"};
	$retweeted_author_id = $rtwdl->{"user_id_str"};
	unless ( defined($retweeted_author_id) ) {
	    print STDERR "tweet $id retweeting $retweeted_id gives no author id\n";
	    last RETWEETED_IF;
	}
	unless ( defined($rtwd->{"core"}->{"user_results"}->{"result"})
		 && defined($rtwd->{"core"}->{"user_results"}->{"result"}->{"__typename"})
		 && ($rtwd->{"core"}->{"user_results"}->{"result"}->{"__typename"} eq "User")
		 && defined($rtwd->{"core"}->{"user_results"}->{"result"}->{"rest_id"})
		 && ($rtwd->{"core"}->{"user_results"}->{"result"}->{"rest_id"} eq $retweeted_author_id) ) {
	    print STDERR "tweet $id retweeting $retweeted_id gives bad or missing author object\n";
	    last RETWEETED_IF;
	}
	my $retweeted_author_screen_name = $rtwd->{"core"}->{"user_results"}->{"result"}->{"legacy"}->{"screen_name"};
	unless ( defined($retweeted_author_screen_name) ) {
	    print STDERR "tweet $id retweeting $retweeted_id gives no author screen name\n";
	    last RETWEETED_IF;
	}
    }}
    ## Quoted
    my $quoted_r;
    my ($quoted_id, $quoted_author_id, $quoted_author_screen_name);
    $quoted_id = $rl->{"quoted_status_id_str"};
    my $quoted_permalink = $rl->{"quoted_status_permalink"}->{"expanded"};
    if ( defined($quoted_id) ) {QUOTED_IF:{
	my $qtwd = $r->{"quoted_status_result"}->{"result"};
	# if ( defined($qtwd)
	#      && defined($qtwd->{"__typename"})
	#      && ($qtwd->{"__typename"} eq "TweetTombstone") ) {
	#     last QUOTED_IF;
	# }
	unless ( defined($qtwd)
		 && defined($qtwd->{"__typename"})
		 && ($qtwd->{"__typename"} eq "Tweet") ) {
	    last QUOTED_IF;
	}
	unless ( defined($qtwd->{"rest_id"})
		 && ($qtwd->{"rest_id"} eq $quoted_id) ) {
	    print STDERR "tweet $id quoting another does not give quoted id\n";
	    last QUOTED_IF;
	}
	$quoted_r = $qtwd;
	my $qtwdl = $qtwd->{"legacy"};
	$quoted_author_id = $qtwdl->{"user_id_str"};
	unless ( defined($quoted_author_id) ) {
	    print STDERR "tweet $id quoting $quoted_id gives no author id\n";
	    last QUOTED_IF;
	}
	unless ( defined($qtwd->{"core"}->{"user_results"}->{"result"})
		 && defined($qtwd->{"core"}->{"user_results"}->{"result"}->{"__typename"})
		 && ($qtwd->{"core"}->{"user_results"}->{"result"}->{"__typename"} eq "User")
		 && defined($qtwd->{"core"}->{"user_results"}->{"result"}->{"rest_id"})
		 && ($qtwd->{"core"}->{"user_results"}->{"result"}->{"rest_id"} eq $quoted_author_id) ) {
	    print STDERR "tweet $id quoting $quoted_id gives bad or missing author object\n";
	    last QUOTED_IF;
	}
	my $quoted_author_screen_name = $qtwd->{"core"}->{"user_results"}->{"result"}->{"legacy"}->{"screen_name"};
	unless ( defined($quoted_author_screen_name) ) {
	    print STDERR "tweet $id quoting $quoted_id gives no author screen name\n";
	    last QUOTED_IF;
	}
    }}
    ## Text
    my $full_text = $rl->{"full_text"};
    unless ( defined($full_text) ) {
	print STDERR "tweet $id has no text: aborting\n";
	return;
    }
    # Attempt to reconstruct tweet input text
    my $input_text = $full_text;
  SUBSTITUTE:{
      if ( $input_text =~ m/[\<\>]|\&(?!lt\;|gt\;|amp\;|apos\;|quot\;)/ ) {
	  # Old tweets may have unescaped HTML.  Leave them alone!
	  print STDERR "tweet $id contains unescaped HTML\n"
	      if $created_at_iso ge "2018";
	  last SUBSTITUTE;
      }
      my @substitutions;
      my $found_quoted_permalink_in_entities = 0;
      unless ( defined($rl->{"entities"}) ) {
	  print STDERR "tweet $id has no entities part\n";
	  last SUBSTITUTE;
      }
      if ( defined($rl->{"entities"}->{"urls"}) ) {
	  for my $ent ( @{$rl->{"entities"}->{"urls"}} ) {
	      my $idx0 = $ent->{"indices"}->[0];
	      my $idx1 = $ent->{"indices"}->[1];
	      push @substitutions, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"expanded_url"})];
	      if ( defined($quoted_permalink)
		   && $quoted_permalink eq $ent->{"expanded_url"} ) {
		  $found_quoted_permalink_in_entities = 1;
	      }
	  }
      }
      if ( defined($rl->{"extended_entities"}->{"media"}) ) {
	  my $previdx0;
	  for my $ent ( @{$rl->{"entities"}->{"media"}} ) {
	      my $idx0 = $ent->{"indices"}->[0];
	      my $idx1 = $ent->{"indices"}->[1];
	      # Multiple media entities may subtitute the same part of
	      # the tweet: do this only once.
	      push @substitutions, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"display_url"})]
		  unless defined($previdx0) && $idx0==$previdx0;
	      $previdx0 = $idx0;
	  }
      }
      $input_text = substitute_in_string $input_text, \@substitutions;
      # If the permalink of the quoted tweet was not found in the URLs
      # being substituted, add it explicitly at the end.
      if ( defined($quoted_permalink)
	   && ! $found_quoted_permalink_in_entities ) {
	  $input_text .= (($input_text=~m/\s\z/)?"":" ") . $quoted_permalink;
      }
      $input_text = html_unquote $input_text;
    }
    ## Miscellaneous
    my $lang = $rl->{"lang"};
    my $favorite_count = $rl->{"favorite_count"};
    my $retweet_count = $rl->{"retweet_count"};
    my $quote_count = $rl->{"quote_count"};
    my $reply_count = $rl->{"reply_count"};
    ## Insert
    $dbh->{AutoCommit} = 0;
    my $sth = $weak ? $weak_insert_tweet_sth : $insert_tweet_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $created_at, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(3, $author_id, { pg_type => PG_TEXT });
    $sth->bind_param(4, $author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(5, $conversation_id, { pg_type => PG_TEXT });
    $sth->bind_param(6, $thread_id, { pg_type => PG_TEXT });
    $sth->bind_param(7, $replyto_id, { pg_type => PG_TEXT });
    $sth->bind_param(8, $replyto_author_id, { pg_type => PG_TEXT });
    $sth->bind_param(9, $replyto_author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(10, $retweeted_id, { pg_type => PG_TEXT });
    $sth->bind_param(11, $retweeted_author_id, { pg_type => PG_TEXT });
    $sth->bind_param(12, $retweeted_author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(13, $quoted_id, { pg_type => PG_TEXT });
    $sth->bind_param(14, $quoted_author_id, { pg_type => PG_TEXT });
    $sth->bind_param(15, $quoted_author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(16, $full_text, { pg_type => PG_TEXT });
    $sth->bind_param(17, $input_text, { pg_type => PG_TEXT });
    $sth->bind_param(18, $lang, { pg_type => PG_TEXT });
    $sth->bind_param(19, $favorite_count, SQL_INTEGER);
    $sth->bind_param(20, $retweet_count, SQL_INTEGER);
    $sth->bind_param(21, $quote_count, SQL_INTEGER);
    $sth->bind_param(22, $reply_count, SQL_INTEGER);
    $sth->bind_param(23, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    my $ret = $sth->fetchall_arrayref;
    die "something went very wrong" unless $weak || ($ret->[0][0] eq $id);
    my $meta_date = $ret->[0][1] // "now";
    $sth = $weak ? $weak_insert_authority_sth : $insert_authority_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $orig, { pg_type => PG_TEXT });
    $sth->bind_param(3, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(4, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(5, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    $ret = $sth->fetchall_arrayref;
    die "something went very wrong" unless $weak || ($ret->[0][0] eq $id);
    die "something went very wrong" unless $weak || ($ret->[0][1] eq $meta_date);
    $dbh->commit;
    ## Process media, author, and retweeted or quoted tweet
    my $media_lst_r = ($rl->{"extended_entities"}->{"media"}) // ($rl->{"entities"}->{"media"});
    if ( defined($media_lst_r) && ref($media_lst_r) eq "ARRAY" ) {
	foreach my $media_r ( @{$media_lst_r} ) {
	    record_media($media_r, $weak, $id);
	}
    }
    record_user($author_r, $weak) if defined($author_r);
    record_tweet($retweeted_r, 1) if defined($retweeted_r);
    record_tweet($quoted_r, 1) if defined($quoted_r);
}

sub record_media {
    # Insert media entry into database.  Arguments are the ref to the
    # media entry's (decoded) JSON, a weak parameter indicating
    # whether we should leave existing entries, and the id of the
    # caller tweet.
    my $r = shift;
    my $weak = shift || $global_weak;  # If 1 leave existing records be
    my $caller_id = shift;
    my $orig = $json_coder_unicode->encode($r);
    ## Basic stuff
    my $id = $r->{"id_str"};
    unless ( defined($id) ) {
	print STDERR "media from tweet $caller_id has no id: aborting\n";
	return;
    }
    unless ( $id =~ m/\A[0-9]+\z/ ) {
	print STDERR "media from tweet $caller_id has badly formed id: aborting\n";
	return;
    }
    my $parent_id = $r->{"source_status_id_str"} // $caller_id;
    my $short_url = $r->{"url"};
    unless ( defined($short_url) ) {
	print STDERR "media $id has no short url: aborting\n";
	return;
    }
    my $display_url = $r->{"display_url"};
    unless ( defined($display_url) ) {
	print STDERR "media $id has no display url: aborting\n";
	return;
    }
    my $media_url = $r->{"media_url_https"};
    my $media_type = $r->{"type"};
    unless ( defined($media_type) ) {
	print STDERR "media $id has no type: aborting\n";
	return;
    }
    my $alt_text = $r->{"ext_alt_text"};
    ## Insert
    $dbh->{AutoCommit} = 0;
    my $sth = $weak ? $weak_insert_media_sth : $insert_media_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $parent_id, { pg_type => PG_TEXT });
    $sth->bind_param(3, $short_url, { pg_type => PG_TEXT });
    $sth->bind_param(4, $display_url, { pg_type => PG_TEXT });
    $sth->bind_param(5, $media_url, { pg_type => PG_TEXT });
    $sth->bind_param(6, $media_type, { pg_type => PG_TEXT });
    $sth->bind_param(7, $alt_text, { pg_type => PG_TEXT });
    $sth->bind_param(8, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    my $ret = $sth->fetchall_arrayref;
    die "something went very wrong" unless $weak || ($ret->[0][0] eq $id);
    my $meta_date = $ret->[0][1] // "now";
    $sth = $weak ? $weak_insert_authority_sth : $insert_authority_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $orig, { pg_type => PG_TEXT });
    $sth->bind_param(3, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(4, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(5, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    $ret = $sth->fetchall_arrayref;
    die "something went very wrong" unless $weak || ($ret->[0][0] eq $id);
    die "something went very wrong" unless $weak || ($ret->[0][1] eq $meta_date);
    $dbh->commit;
}

sub record_user {
    # Insert user into database.  Arguments are the ref to the user's
    # (decoded) JSON, and a weak parameter indicating whether we
    # should leave existing entries.
    my $r = shift;
    my $weak = shift || $global_weak;  # If 1 leave existing records be
    my $orig = $json_coder_unicode->encode($r);
    ## Basic stuff
    my $id = $r->{"rest_id"};
    unless ( defined($id) ) {
	print STDERR "user has no id: aborting\n";
	return;
    }
    unless ( $id =~ m/\A[0-9]+\z/ ) {
	print STDERR "user has badly formed id: aborting\n";
	return;
    }
    my $rl = $r->{"legacy"};
    unless ( defined($rl) && ref($rl) eq "HASH" ) {
	print STDERR "user $id has no legacy field: aborting\n";
	return;
    }
    my $created_at_str = $rl->{"created_at"};
    unless ( defined($created_at_str) ) {
	print STDERR "user $id has no creation date: aborting\n";
	return;
    }
    my $created_at = $datetime_parser->parse_datetime($created_at_str);
    unless ( defined($created_at) ) {
	print STDERR "user $id has invalid creation date: aborting\n";
	return;
    }
    my $screen_name = $rl->{"screen_name"};
    my $full_name = $rl->{"name"};
    ## Description
    my $profile_description = $rl->{"description"};
    my $profile_input_description;
    if ( defined($profile_description) ) {SUBSTITUTE:{
	$profile_input_description = $profile_description;
	my @substitutions;
	unless ( defined($rl->{"entities"}->{"description"}) ) {
	    print STDERR "user $id has no entities part\n";
	    last SUBSTITUTE;
	}
	for my $ent ( @{$rl->{"entities"}->{"description"}->{"urls"}} ) {
	    my $idx0 = $ent->{"indices"}->[0];
	    my $idx1 = $ent->{"indices"}->[1];
	    # NO html_quote here (the description is NOT html-encoded)
	    push @substitutions, [$idx0, $idx1-$idx0, $ent->{"url"}, $ent->{"expanded_url"}];
	}
	$profile_input_description = substitute_in_string $profile_input_description, \@substitutions;
    }}
    ## Miscellaneous
    my $profile_url = $rl->{"entities"}->{"url"}->{"urls"}->[0]->{"expanded_url"};
    my $pinned_id = $rl->{"pinned_tweet_ids_str"}->[0];
    my $followers_count = $rl->{"followers_count"};
    my $following_count = $rl->{"friends_count"};
    my $statuses_count = $rl->{"statuses_count"};
    ## Insert
    $dbh->{AutoCommit} = 0;
    my $sth = $weak ? $weak_insert_user_sth : $insert_user_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $created_at, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(3, $screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(4, $full_name, { pg_type => PG_TEXT });
    $sth->bind_param(5, $profile_description, { pg_type => PG_TEXT });
    $sth->bind_param(6, $profile_input_description, { pg_type => PG_TEXT });
    $sth->bind_param(7, $profile_url, { pg_type => PG_TEXT });
    $sth->bind_param(8, $pinned_id, { pg_type => PG_TEXT });
    $sth->bind_param(9, $followers_count, SQL_INTEGER);
    $sth->bind_param(10, $following_count, SQL_INTEGER);
    $sth->bind_param(11, $statuses_count, SQL_INTEGER);
    $sth->bind_param(12, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    my $ret = $sth->fetchall_arrayref;
    die "something went very wrong" unless $weak || ($ret->[0][0] eq $id);
    my $meta_date = $ret->[0][1] // "now";
    $sth = $weak ? $weak_insert_authority_sth : $insert_authority_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $orig, { pg_type => PG_TEXT });
    $sth->bind_param(3, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(4, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(5, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    $ret = $sth->fetchall_arrayref;
    die "something went very wrong" unless $weak || ($ret->[0][0] eq $id);
    die "something went very wrong" unless $weak || ($ret->[0][1] eq $meta_date);
    $dbh->commit;
}

sub generic_recurse {
    # Recurse into JSON structure, calling record_tweet or record_user
    # on whatever looks like it should be inserted.
    my $r = shift;
    if ( ref($r) eq "ARRAY" ) {
	for ( my $i=0 ; $i<scalar(@{$r}) ; $i++ ) {
	    generic_recurse ($r->[$i]);
	}
    } elsif ( ref($r) eq "HASH" ) {
	if ( defined($r->{"__typename"}) && $r->{"__typename"} eq "Tweet"
	     && defined($r->{"rest_id"}) ) {
	    record_tweet($r, 0);
	    return;  # Recusion is done inside record_tweet
	}
	if ( defined($r->{"__typename"}) && $r->{"__typename"} eq "User"
	     && defined($r->{"rest_id"}) ) {
	    record_user($r, 0);
	    return;
	}
	foreach my $k ( keys(%{$r}) ) {
	    generic_recurse ($r->{$k});
	}
    }
}

sub process_content {
    my $content = shift;
    my $data = $json_decoder_utf8->decode($content);
    if ( $har_mode ) {
	die "bad HAR format"
	    unless defined($data->{"log"}->{"entries"})
	    && ref($data->{"log"}->{"entries"}) eq "ARRAY";
	foreach my $ent ( @{$data->{"log"}->{"entries"}} ) {
	    printf STDERR "processing request made at %s\n", $ent->{"startedDateTime"};
	    if ( $ent->{"response"}->{"status"} == 200
		 && ( $ent->{"response"}->{"content"}->{"mimeType"}
		      =~ m/^application\/json(?:\;|$)/ ) ) {
		my $subcontent = $ent->{"response"}->{"content"}->{"text"};
		my $subdata = $json_decoder_unicode->decode($subcontent);
		generic_recurse $subdata;
	    }
	}
    } else {
	generic_recurse $data;
    }
}

if ( scalar(@ARGV) ) {
    foreach my $fname ( @ARGV ) {
	open my $f, "<", $fname
	    or die "can't open $fname: $!";
	print STDERR "processing file $fname\n";
	local $/ = undef;  # enable localized slurp mode
	my $content = <$f>;
	process_content $content;
	close $f;
    }
} else {
    local $/ = undef;  # enable localized slurp mode
    my $content = <STDIN>;
    process_content $content;
}
