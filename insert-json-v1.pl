#! /usr/local/bin/perl -w

use strict;
use warnings;

use Getopt::Std;

use POSIX qw(strftime);

use JSON::XS;
use DateTime::Format::Strptime;

use DBI qw(:sql_types);
use DBD::Pg qw(:pg_types);

my %opts;
getopts('d:hws:', \%opts);

my $har_mode = $opts{h};

my $global_weak = $opts{w};
my $global_source = $opts{s} // "json-feed-v1";

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

my $dbname = $opts{d} // "twitter";
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
	. "( id , obj_type , orig "
	. ", meta_updated_at , meta_inserted_at , meta_source "
	. ", auth_source , auth_date ) "
	. "VALUES ( ?,?,?::json,?,?,?,?,? ) ";
    my $conflict = "ON CONFLICT ON CONSTRAINT authority_meta_source_id_key DO UPDATE SET "
	. "id = EXCLUDED.id "
	. ", obj_type = EXCLUDED.obj_type "
	. ", orig = EXCLUDED.orig "
	. ", meta_updated_at = EXCLUDED.meta_updated_at "
	. ", meta_source = EXCLUDED.meta_source "
	. ", auth_source = EXCLUDED.auth_source "
	. ", auth_date = EXCLUDED.auth_date ";
    my $weak_conflict = "ON CONFLICT ON CONSTRAINT authority_meta_source_id_key DO UPDATE SET "
	# This is a deliberate no-op, but we can't DO NOTHING because
	# we want to return the id line.
	. "id = authority.id ";
    my $returning = "RETURNING id , meta_updated_at";
    $insert_authority_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_authority_sth = $dbh->prepare($command . $weak_conflict . $returning);
    # Prepare command to insert into "tweets" table:
    $command = "INSERT INTO tweets "
	. "( id , created_at , author_id , author_screen_name "
	. ", conversation_id , thread_id "
	. ", replyto_id , replyto_author_id , replyto_author_screen_name "
	. ", retweeted_id , retweeted_author_id , retweeted_author_screen_name "
	. ", quoted_id , quoted_author_id , quoted_author_screen_name "
	. ", full_text , input_text , lang "
	. ", favorite_count , retweet_count , quote_count , reply_count "
	. ", meta_updated_at , meta_source ) "
	. "VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? ) ";
    $conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "created_at = EXCLUDED.created_at "
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
    $weak_conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "conversation_id = COALESCE(tweets.conversation_id, EXCLUDED.conversation_id) "
	. ", thread_id = COALESCE(tweets.thread_id, EXCLUDED.thread_id) "
	. ", replyto_id = COALESCE(tweets.replyto_id, EXCLUDED.replyto_id) "
	. ", replyto_author_id = COALESCE(tweets.replyto_author_id, EXCLUDED.replyto_author_id) "
	. ", replyto_author_screen_name = COALESCE(tweets.replyto_author_screen_name, EXCLUDED.replyto_author_screen_name) "
	. ", retweeted_id = COALESCE(tweets.retweeted_id, EXCLUDED.retweeted_id) "
	. ", retweeted_author_id = COALESCE(tweets.retweeted_author_id, EXCLUDED.retweeted_author_id) "
	. ", retweeted_author_screen_name = COALESCE(tweets.retweeted_author_screen_name, EXCLUDED.retweeted_author_screen_name) "
	. ", quoted_id = COALESCE(tweets.quoted_id, EXCLUDED.quoted_id) "
	. ", quoted_author_id = COALESCE(tweets.quoted_author_id, EXCLUDED.quoted_author_id) "
	. ", quoted_author_screen_name = COALESCE(tweets.quoted_author_screen_name, EXCLUDED.quoted_author_screen_name) "
	. ", lang = COALESCE(tweets.lang, EXCLUDED.lang) "
	. ", favorite_count = COALESCE(tweets.favorite_count, EXCLUDED.favorite_count) "
	. ", retweet_count = COALESCE(tweets.retweet_count, EXCLUDED.retweet_count) "
	. ", quote_count = COALESCE(tweets.quote_count, EXCLUDED.quote_count) "
	. ", reply_count = COALESCE(tweets.reply_count, EXCLUDED.reply_count) "
	. ", meta_updated_at = EXCLUDED.meta_updated_at "
	. ", meta_source = EXCLUDED.meta_source ";
    $returning = "RETURNING id , meta_updated_at";
    $insert_tweet_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_tweet_sth = $dbh->prepare($command . $weak_conflict . $returning);
    # Prepare command to insert into "media" table:
    $command = "INSERT INTO media "
	. "( id , parent_id , parent_author_id , parent_author_screen_name "
	. ", short_url , display_url "
	. ", media_url , media_type , alt_text "
	. ", meta_updated_at , meta_source ) "
	. "VALUES ( ?,?,?,?,?,?,?,?,?,?,? ) ";
    $conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "parent_id = EXCLUDED.parent_id "
	. ", parent_author_id = EXCLUDED.parent_author_id "
	. ", parent_author_screen_name = COALESCE(EXCLUDED.parent_author_screen_name, media.parent_author_screen_name) "
	. ", short_url = EXCLUDED.short_url "
	. ", display_url = EXCLUDED.display_url "
	. ", media_url = COALESCE(EXCLUDED.media_url, media.media_url) "
	. ", media_type = EXCLUDED.media_type "
	. ", alt_text = COALESCE(EXCLUDED.alt_text, media.alt_text) "
	. ", meta_updated_at = EXCLUDED.meta_updated_at "
	. ", meta_source = EXCLUDED.meta_source ";
    $weak_conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "parent_author_screen_name = COALESCE(media.parent_author_screen_name, EXCLUDED.parent_author_screen_name) "
	. ", media_url = COALESCE(media.media_url, EXCLUDED.media_url) "
	. ", alt_text = COALESCE(media.alt_text, EXCLUDED.alt_text) "
	. ", meta_updated_at = EXCLUDED.meta_updated_at "
	. ", meta_source = EXCLUDED.meta_source ";
    $insert_media_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_media_sth = $dbh->prepare($command . $weak_conflict . $returning);
    # Prepare command to insert into "users" table:
    $command = "INSERT INTO users "
	. "( id , created_at , screen_name , full_name "
	. ", profile_description , profile_input_description , profile_url "
	. ", pinned_id , followers_count , following_count , statuses_count "
	. ", meta_updated_at , meta_source ) "
	. "VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,? ) ";
    $conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "created_at = EXCLUDED.created_at "
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
    $weak_conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "full_name = COALESCE(users.full_name, EXCLUDED.full_name) "
	. ", profile_description = COALESCE(users.profile_description, EXCLUDED.profile_description) "
	. ", profile_input_description = COALESCE(users.profile_input_description, EXCLUDED.profile_input_description) "
	. ", profile_url = COALESCE(users.profile_url, EXCLUDED.profile_url) "
	. ", pinned_id = COALESCE(users.pinned_id, EXCLUDED.pinned_id) "
	. ", followers_count = COALESCE(users.followers_count, EXCLUDED.followers_count) "
	. ", following_count = COALESCE(users.following_count, EXCLUDED.following_count) "
	. ", statuses_count = COALESCE(users.statuses_count, EXCLUDED.statuses_count) "
	. ", meta_updated_at = EXCLUDED.meta_updated_at "
	. ", meta_source = EXCLUDED.meta_source ";
    $insert_user_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_user_sth = $dbh->prepare($command . $weak_conflict . $returning);
}

do_connect;

my $global_auth_source;
my $global_auth_date;

my %global_quick_user_screen_names;

sub record_tweet_v1 {
    # Insert tweet into database.  Arguments are the ref to the
    # tweet's (decoded) JSON, and a weak parameter indicating whether
    # we should leave existing entries.
    my $r = shift;
    my $weak = shift || $global_weak;  # If 1 leave existing records be
    ## Basic stuff
    my $id = $r->{"id_str"};
    unless ( defined($id) ) {
	print STDERR "tweet has no id: aborting\n";
	return;
    }
    unless ( $id =~ m/\A[0-9]+\z/ ) {
	print STDERR "tweet has badly formed id: aborting\n";
	return;
    }
    my $rl = $r;  # Simplify synchronization with insert-json.pl
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
    my $author_id = $rl->{"user_id_str"} // $rl->{"user"}->{"id_str"};
    unless ( defined($author_id) ) {
	print STDERR "tweet $id has no author id: aborting\n";
	return;
    }
    my $author_screen_name = $rl->{"user_screen_name"} // $rl->{"user"}->{"screen_name"} // $rl->{"users"}->{$author_id}->{"screen_name"} // $global_quick_user_screen_names{$author_id};
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
    my $retweeted_id;
    $retweeted_id = $rl->{"retweeted_status_id_str"} // $rl->{"retweeted_status"}->{"id_str"};
    ## Quoted
    my ($quoted_id, $quoted_author_screen_name);
    $quoted_id = $rl->{"quoted_status_id_str"};
    my $quoted_permalink = $rl->{"quoted_status_permalink"}->{"expanded"};
    if ( defined($quoted_permalink)
	 && $quoted_permalink =~ /\Ahttps?\:\/\/(?:mobile\.)?twitter\.com\/([A-Za-z0-9\_]+)\/status\/([0-9]+)\z/ ) {
	$quoted_author_screen_name = $1;
	$quoted_id = $quoted_id // $2;
    }
    ## Text
    my $full_text = $rl->{"full_text"};
    my $media_lst_r = ($rl->{"extended_entities"}->{"media"}) // ($rl->{"entities"}->{"media"});
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
      if ( defined($media_lst_r) ) {
	  my $previdx0;
	  for my $ent ( @{$media_lst_r} ) {
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
    ## Create JSON
    my $orig = $json_coder_unicode->encode({
	"__typename" => "Tweet",
	"rest_id" => $id,
	"legacy" => $rl
    });
    ## Insert
    $dbh->{AutoCommit} = 0;
    my $sth = $weak ? $weak_insert_authority_sth : $insert_authority_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, "tweet", { pg_type => PG_TEXT });
    $sth->bind_param(3, $orig, { pg_type => PG_TEXT });
    $sth->bind_param(4, "now", { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(5, "now", { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(6, $global_source, { pg_type => PG_TEXT });
    $sth->bind_param(7, $global_auth_source, { pg_type => PG_TEXT });
    $sth->bind_param(8, $global_auth_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->execute();
    my $ret = $sth->fetchall_arrayref;
    die "insertion into database failed" unless defined($ret->[0][0]) && ($ret->[0][0] eq $id);
    my $meta_date = $ret->[0][1] // "now";
    $sth = $weak ? $weak_insert_tweet_sth : $insert_tweet_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $created_at_iso, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(3, $author_id, { pg_type => PG_TEXT });
    $sth->bind_param(4, $author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(5, $conversation_id, { pg_type => PG_TEXT });
    $sth->bind_param(6, $thread_id, { pg_type => PG_TEXT });
    $sth->bind_param(7, $replyto_id, { pg_type => PG_TEXT });
    $sth->bind_param(8, $replyto_author_id, { pg_type => PG_TEXT });
    $sth->bind_param(9, $replyto_author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(10, $retweeted_id, { pg_type => PG_TEXT });
    $sth->bind_param(11, undef, { pg_type => PG_TEXT });
    $sth->bind_param(12, undef, { pg_type => PG_TEXT });
    $sth->bind_param(13, $quoted_id, { pg_type => PG_TEXT });
    $sth->bind_param(14, undef, { pg_type => PG_TEXT });
    $sth->bind_param(15, $quoted_author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(16, $full_text, { pg_type => PG_TEXT });
    $sth->bind_param(17, $input_text, { pg_type => PG_TEXT });
    $sth->bind_param(18, $lang, { pg_type => PG_TEXT });
    $sth->bind_param(19, $favorite_count, SQL_INTEGER);
    $sth->bind_param(20, $retweet_count, SQL_INTEGER);
    $sth->bind_param(21, $quote_count, SQL_INTEGER);
    $sth->bind_param(22, $reply_count, SQL_INTEGER);
    $sth->bind_param(23, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(24, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    $ret = $sth->fetchall_arrayref;
    die "insertion into database failed" unless defined($ret->[0][0]) && ($ret->[0][0] eq $id);
    $dbh->commit;
    ## Process media, author, and retweeted or quoted tweet
    if ( defined($media_lst_r) && ref($media_lst_r) eq "ARRAY" ) {
	foreach my $media_r ( @{$media_lst_r} ) {
	    record_media($media_r, $weak, $id, $author_id, $author_screen_name);
	}
    }
    if ( defined($rl->{"user"}) ) {
	my $author_r = $rl->{"user"};
	if ( scalar(keys(%{$author_r})) ) {
	    record_user_v1($author_r, $weak);
	}
    }
    if ( defined($rl->{"users"}) ) {
	my $rr = $rl->{"users"};
	foreach my $k ( keys(%{$rr}) ) {
	    if ( $k =~ m/\A[0-9]+\z/ ) {
		next unless scalar(keys(%{$rr->{$k}}));
		record_user_v1($rr->{$k}, $weak);
	    } else {
		print STDERR "ignoring nonsensical key $k in users structure\n";
	    }
	}
    }
}

sub record_media {
    # Insert media entry into database.  Arguments are the ref to the
    # media entry's (decoded) JSON, a weak parameter indicating
    # whether we should leave existing entries, and the id of the
    # caller tweet, caller tweet's user and caller tweet's user screen
    # name.
    my $r = shift;
    my $weak = shift || $global_weak;  # If 1 leave existing records be
    my $caller_id = shift;
    my $caller_author_id = shift;
    my $caller_author_screen_name = shift;
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
    my $parent_author_id = $r->{"source_user_id_str"} // $caller_author_id;
    my $parent_author_screen_name;
    my $expanded_url = $r->{"expanded_url"};
    if ( defined($expanded_url)
	 && $expanded_url =~ /\Ahttps?\:\/\/(?:mobile\.)?twitter\.com\/([A-Za-z0-9\_]+)\/status\/([0-9]+)\// ) {
	$parent_author_screen_name = $1;
	if ( $parent_id ne $2 ) {
	    print STDERR "media $id has unexpected parent id in expanded url\n";
	}
    }
    $parent_author_screen_name = $parent_author_screen_name // $caller_author_screen_name;
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
    ## Create JSON
    my $orig = $json_coder_unicode->encode($r);
    ## Insert
    $dbh->{AutoCommit} = 0;
    my $sth = $weak ? $weak_insert_authority_sth : $insert_authority_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, "media", { pg_type => PG_TEXT });
    $sth->bind_param(3, $orig, { pg_type => PG_TEXT });
    $sth->bind_param(4, "now", { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(5, "now", { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(6, $global_source, { pg_type => PG_TEXT });
    $sth->bind_param(7, $global_auth_source, { pg_type => PG_TEXT });
    $sth->bind_param(8, $global_auth_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->execute();
    my $ret = $sth->fetchall_arrayref;
    die "insertion into database failed" unless defined($ret->[0][0]) && ($ret->[0][0] eq $id);
    my $meta_date = $ret->[0][1] // "now";
    $sth = $weak ? $weak_insert_media_sth : $insert_media_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $parent_id, { pg_type => PG_TEXT });
    $sth->bind_param(3, $parent_author_id, { pg_type => PG_TEXT });
    $sth->bind_param(4, $parent_author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(5, $short_url, { pg_type => PG_TEXT });
    $sth->bind_param(6, $display_url, { pg_type => PG_TEXT });
    $sth->bind_param(7, $media_url, { pg_type => PG_TEXT });
    $sth->bind_param(8, $media_type, { pg_type => PG_TEXT });
    $sth->bind_param(9, $alt_text, { pg_type => PG_TEXT });
    $sth->bind_param(10, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(11, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    $ret = $sth->fetchall_arrayref;
    die "insertion into database failed" unless defined($ret->[0][0]) && ($ret->[0][0] eq $id);
    $dbh->commit;
    ## Some media include a source user:
    if ( defined($r->{"additional_media_info"}->{"source_user"}->{"id_str"}) ) {
	my $parent_author_r = $r->{"additional_media_info"}->{"source_user"};
	record_user_v1($parent_author_r, $weak);
    }
}

sub record_user_v1 {
    # Insert user into database.  Arguments are the ref to the user's
    # (decoded) JSON, and a weak parameter indicating whether we
    # should leave existing entries.
    my $r = shift;
    my $weak = shift || $global_weak;  # If 1 leave existing records be
    ## Basic stuff
    my $id = $r->{"id_str"};
    unless ( defined($id) ) {
	print STDERR "user has no id: aborting\n";
	return;
    }
    unless ( $id =~ m/\A[0-9]+\z/ ) {
	print STDERR "user has badly formed id: aborting\n";
	return;
    }
    my $rl = $r;  # Simplify synchronization with insert-json.pl
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
    my $created_at_iso = $created_at->strftime("%Y-%m-%d %H:%M:%S+00:00");
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
    ## Create JSON
    my $orig = $json_coder_unicode->encode({
	"__typename" => "User",
	"rest_id" => $id,
	"legacy" => $rl
    });
    ## Insert
    $dbh->{AutoCommit} = 0;
    my $sth = $weak ? $weak_insert_authority_sth : $insert_authority_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, "user", { pg_type => PG_TEXT });
    $sth->bind_param(3, $orig, { pg_type => PG_TEXT });
    $sth->bind_param(4, "now", { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(5, "now", { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(6, $global_source, { pg_type => PG_TEXT });
    $sth->bind_param(7, $global_auth_source, { pg_type => PG_TEXT });
    $sth->bind_param(8, $global_auth_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->execute();
    my $ret = $sth->fetchall_arrayref;
    die "insertion into database failed" unless defined($ret->[0][0]) && ($ret->[0][0] eq $id);
    my $meta_date = $ret->[0][1] // "now";
    $sth = $weak ? $weak_insert_user_sth : $insert_user_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $created_at_iso, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(3, $screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(4, $full_name, { pg_type => PG_TEXT });
    $sth->bind_param(5, $profile_description, { pg_type => PG_TEXT });
    $sth->bind_param(6, $profile_input_description, { pg_type => PG_TEXT });
    $sth->bind_param(7, $profile_url, { pg_type => PG_TEXT });
    $sth->bind_param(8, $pinned_id, { pg_type => PG_TEXT });
    $sth->bind_param(9, $followers_count, SQL_INTEGER);
    $sth->bind_param(10, $following_count, SQL_INTEGER);
    $sth->bind_param(11, $statuses_count, SQL_INTEGER);
    $sth->bind_param(12, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(13, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    $ret = $sth->fetchall_arrayref;
    die "insertion into database failed" unless defined($ret->[0][0]) && ($ret->[0][0] eq $id);
    $dbh->commit;
    $global_quick_user_screen_names{$id} = $screen_name;
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
	if ( defined($r->{"users"}) ) {
	    my $rr = $r->{"users"};
	    foreach my $k ( keys(%{$rr}) ) {
		if ( $k =~ m/\A[0-9]+\z/ ) {
		    record_user_v1($rr->{$k}, 0)
		} else {
		    print STDERR "ignoring nonsensical key $k in users structure\n";
		}
	    }
	}
	if ( defined($r->{"tweets"}) ) {
	    my $rr = $r->{"tweets"};
	    foreach my $k ( keys(%{$rr}) ) {
		if ( $k =~ m/\A[0-9]+\z/ ) {
		    record_tweet_v1($rr->{$k}, 0)
		} else {
		    print STDERR "ignoring nonsensical key $k in tweets structure\n";
		}
	    }
	}
	foreach my $k ( keys(%{$r}) ) {
	    next if $k eq "users" || $k eq "tweets";
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
		next if $subcontent eq "";
		my $subdata = $json_decoder_unicode->decode($subcontent);
		$global_auth_date = $ent->{"startedDateTime"};
		%global_quick_user_screen_names = ();
		generic_recurse $subdata;
	    }
	}
    } else {
	%global_quick_user_screen_names = ();
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
	$global_auth_source = $fname;
	$global_auth_date = strftime("%Y-%m-%d %H:%M:%S+00:00", gmtime((stat($f))[9]));
	process_content $content;
	close $f;
    }
} else {
    local $/ = undef;  # enable localized slurp mode
    my $content = <STDIN>;
    $global_auth_date = strftime("%Y-%m-%d %H:%M:%S+00:00", gmtime((stat(STDIN))[9]));
    process_content $content;
}
