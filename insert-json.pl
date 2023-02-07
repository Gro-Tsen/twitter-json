#! /usr/local/bin/perl -w

use strict;
use warnings;

use JSON::XS;
use DateTime::Format::Strptime;

use DBI qw(:sql_types);
use DBD::Pg qw(:pg_types);

binmode STDOUT, ":utf8";

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
	die "Attempting to overlap substitutions" unless $idx0>=$minbar;
	my $len = $sb->[1];
	if ( defined($sb->[2]) ) {
	    die(sprintf("Bad calibration: expecting \"%s\", got \"%s\"\n", $sb->[2], substr($str, $idx0, $len))) unless substr($str, $idx0, $len) eq $sb->[2];
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
my $insert_tweet_sth;
my $weak_insert_tweet_sth;

sub do_connect {
    $dbh = DBI->connect("dbi:Pg:dbname=$dbname", "", "", {AutoCommit=>1,RaiseError=>1,pg_enable_utf8=>1});
    die ("Can't connect to database: " . $DBI::errstr . "\n") unless $dbh;
    $dbh->do("SET TIME ZONE 0");
    die ("Can't set timezone: " . $dbh->errstr . "\n") if $dbh->err;
    my $command = "INSERT INTO tweets "
	. "( id , created_at , author_id , author_screen_name "
	. ", conversation_id , thread_id "
	. ", replyto_id , replyto_author_id , replyto_author_screen_name "
	. ", retweeted_id , retweeted_author_id , retweeted_author_screen_name "
	. ", quoted_id , quoted_author_id , quoted_author_screen_name "
	. ", full_text , input_text , lang "
	. ", favorite_count , quote_count , reply_count "
	. ", orig , meta_source ) "
	. "VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::json,? ) ";
    my $conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "id = EXCLUDED.id "
	. ", created_at = EXCLUDED.created_at "
	. ", author_id = EXCLUDED.author_id "
	. ", author_screen_name = EXCLUDED.author_screen_name "
	. ", conversation_id = EXCLUDED.conversation_id "
	. ", thread_id = EXCLUDED.thread_id "
	. ", replyto_id = EXCLUDED.replyto_id "
	. ", replyto_author_id = EXCLUDED.replyto_author_id "
	. ", replyto_author_screen_name = EXCLUDED.replyto_author_screen_name "
	. ", retweeted_id = EXCLUDED.retweeted_id "
	. ", retweeted_author_id = EXCLUDED.retweeted_author_id "
	. ", retweeted_author_screen_name = EXCLUDED.retweeted_author_screen_name "
	. ", quoted_id = EXCLUDED.quoted_id "
	. ", quoted_author_id = EXCLUDED.quoted_author_id "
	. ", quoted_author_screen_name = EXCLUDED.quoted_author_screen_name "
	. ", full_text = EXCLUDED.full_text "
	. ", input_text = EXCLUDED.input_text "
	. ", lang = EXCLUDED.lang "
	. ", favorite_count = EXCLUDED.favorite_count "
	. ", quote_count = EXCLUDED.quote_count "
	. ", reply_count = EXCLUDED.reply_count "
	. ", orig = EXCLUDED.orig "
	. ", meta_source = EXCLUDED.meta_source "
	. ", meta_updated_at = now() ";
    my $noconflict = "ON CONFLICT ( id ) DO NOTHING ";
    my $returning = "RETURNING id";
    $insert_tweet_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_tweet_sth = $dbh->prepare($command . $noconflict . $returning);
}

do_connect;

sub record_tweet {
    my $r = shift;
    my $weak = shift;  # If 1 leave existing records be
    my $orig = encode_json($r);
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
    my $author_screen_name = $r->{"core"}->{"user_results"}->{"result"}->{"legacy"}->{"screen_name"};
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
	unless ( defined($replyto_author_id) && defined($replyto_author_screen_name) ) {
	    print STDERR "tweet $id replying to $replyto_id does not give replyto author id or screen name\n";
	}
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
	  print STDERR "tweet $id has no entities part";
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
    my $quote_count = $rl->{"quote_count"};
    my $reply_count = $rl->{"reply_count"};
    ## Insert
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
    $sth->bind_param(20, $quote_count, SQL_INTEGER);
    $sth->bind_param(21, $reply_count, SQL_INTEGER);
    $sth->bind_param(22, $orig, { pg_type => PG_TEXT });
    $sth->bind_param(23, 1, SQL_INTEGER);
    $sth->execute();
    ## Process retweeted or quoted tweet
    record_tweet($retweeted_r, 1) if defined($retweeted_r);
    record_tweet($quoted_r, 1) if defined($quoted_r);
}

sub generic_recurse {
    my $r = shift;
    if ( ref($r) eq "ARRAY" ) {
	for ( my $i=0 ; $i<scalar(@{$r}) ; $i++ ) {
	    generic_recurse ($r->[$i]);
	}
    } elsif ( ref($r) eq "HASH" ) {
	if ( defined($r->{"__typename"}) && $r->{"__typename"} eq "Tweet" ) {
	    record_tweet($r, 0);
	    return;  # Recusion is done inside record_tweet
	}
	foreach my $k ( keys(%{$r}) ) {
	    generic_recurse ($r->{$k});
	}
    }
}

my $content;
if (1) {
    local $/ = undef;  # enable localized slurp mode
    $content = <>;
}
my $data = decode_json $content;

generic_recurse $data;
