#! /usr/local/bin/perl -w

# TWEETS_ARCHIVE_USER_ID="1018078984280657920" TWEETS_ARCHIVE_USER_SCREEN_NAME="gro_tsen" ./insert-archive.pl -w /tmp/twitter-archive/data/tweets.js

use strict;
use warnings;

use Getopt::Std;

use JSON::XS;
use DateTime::Format::Strptime;

use DBI qw(:sql_types);
use DBD::Pg qw(:pg_types);

my %opts;
getopts('ws:', \%opts);

my $global_weak = $opts{w};
my $global_source = $opts{s} // "tweets-archive";

my $global_user_id = $ENV{"TWEETS_ARCHIVE_USER_ID"};
my $global_user_screen_name = $ENV{"TWEETS_ARCHIVE_USER_SCREEN_NAME"};
unless ( defined($global_user_id) && defined($global_user_screen_name) ) {
    die "please run this program with TWEETS_ARCHIVE_USER_ID and TWEETS_ARCHIVE_USER_SCREEN_NAME evironment variables set";
}

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
my $insert_tweet_sth;
my $weak_insert_tweet_sth;
my $insert_media_sth;
my $weak_insert_media_sth;

sub do_connect {
    # Connect to database and prepare insert statements.
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
	. ", favorite_count , retweet_count , quote_count , reply_count "
	. ", orig , meta_source ) "
	. "VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::json,? ) ";
    my $conflict = "ON CONFLICT ( id ) DO UPDATE SET "
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
	. ", orig = EXCLUDED.orig "
	. ", meta_updated_at = now() "
	. ", meta_source = EXCLUDED.meta_source ";
    my $noconflict = "ON CONFLICT ( id ) DO NOTHING ";
    my $returning = "RETURNING id";
    $insert_tweet_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_tweet_sth = $dbh->prepare($command . $noconflict . $returning);
    $command = "INSERT INTO media "
	. "( id , parent_id , short_url , display_url "
	. ", media_url , media_type , alt_text "
	. ", orig , meta_source ) "
	. "VALUES ( ?,?,?,?,?,?,?,?::json,? ) ";
    $conflict = "ON CONFLICT ( id ) DO UPDATE SET "
	. "id = EXCLUDED.id "
	. ", parent_id = EXCLUDED.parent_id "
	. ", short_url = EXCLUDED.short_url "
	. ", display_url = EXCLUDED.display_url "
	. ", media_url = COALESCE(EXCLUDED.media_url, media.media_url) "
	. ", media_type = EXCLUDED.media_type "
	. ", alt_text = COALESCE(EXCLUDED.alt_text, media.alt_text) "
	. ", orig = EXCLUDED.orig "
	. ", meta_updated_at = now() "
	. ", meta_source = EXCLUDED.meta_source ";
    $insert_media_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_media_sth = $dbh->prepare($command . $noconflict . $returning);
}

do_connect;

sub record_tweet_v1 {
    # Insert tweet into database.  Arguments are the ref to the
    # tweet's (decoded) JSON, and a weak parameter indicating whether
    # we should leave existing entries.
    my $r = shift;
    my $weak = shift || $global_weak;  # If 1 leave existing records be
    my $orig = encode_json($r);
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
    my $author_id = $rl->{"user"}->{"id_str"} // $global_user_id;
    my $author_screen_name = $rl->{"user"}->{"screen_name"} // $global_user_screen_name;
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
    # Sadly, not present in tweets.js (so this will always give undef):
    $retweeted_id = $rl->{"retweeted_status"}->{"id_str"};
    # $retweeted_author_id, $retweeted_author_screen_name are set below.
    ## Quoted
    # Will be filled by substitutions:
    my ($quoted_id, $quoted_author_screen_name);
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
      unless ( defined($rl->{"entities"}) ) {
	  print STDERR "tweet $id has no entities part\n";
	  last SUBSTITUTE;
      }
      if ( defined($rl->{"entities"}->{"urls"}) ) {
	  for my $ent ( @{$rl->{"entities"}->{"urls"}} ) {
	      my $idx0 = $ent->{"indices"}->[0];
	      my $idx1 = $ent->{"indices"}->[1];
	      if ( $ent->{"expanded_url"} =~ /\Ahttps?\:\/\/(?:mobile\.)?twitter\.com\/([A-Za-z0-9\_]+)\/status\/([0-9]+)\z/ ) {
		  $quoted_id = $2;
		  $quoted_author_screen_name = $1;
	      } elsif ( $ent->{"expanded_url"} =~ /\Ahttps?\:\/\/(?:mobile\.)?twitter\.com\/i\/web\/status\/([0-9]+)\z/ ) {
		  $quoted_id = $1;
	      }
	      push @substitutions, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"expanded_url"})];
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
      $input_text = html_unquote $input_text;
    }
    ## Miscellaneous
    my $lang = $rl->{"lang"};
    my $favorite_count = $rl->{"favorite_count"};
    my $retweet_count = $rl->{"retweet_count"};
    # my $quote_count = $rl->{"quote_count"};
    # my $reply_count = $rl->{"reply_count"};
    ## Insert
    my $sth = $weak ? $weak_insert_tweet_sth : $insert_tweet_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $created_at, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(3, $author_id, { pg_type => PG_TEXT });
    $sth->bind_param(4, $author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(5, undef, { pg_type => PG_TEXT });
    $sth->bind_param(6, undef, { pg_type => PG_TEXT });
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
    $sth->bind_param(21, undef, SQL_INTEGER);
    $sth->bind_param(22, undef, SQL_INTEGER);
    $sth->bind_param(23, $orig, { pg_type => PG_TEXT });
    $sth->bind_param(24, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    ## Process media, author, and retweeted or quoted tweet
    my $media_lst_r = ($rl->{"extended_entities"}->{"media"}) // ($rl->{"entities"}->{"media"});
    if ( defined($media_lst_r) && ref($media_lst_r) eq "ARRAY" ) {
	foreach my $media_r ( @{$media_lst_r} ) {
	    record_media_v1($media_r, $weak, $id);
	}
    }
}

sub record_media_v1 {
    # Insert media entry into database.  Arguments are the ref to the
    # media entry's (decoded) JSON, a weak parameter indicating
    # whether we should leave existing entries, and the id of the
    # caller tweet.
    my $r = shift;
    my $weak = shift || $global_weak;  # If 1 leave existing records be
    my $caller_id = shift;
    my $orig = encode_json($r);
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
    # Sadly, not present in tweets.js (so this will always give undef):
    my $alt_text = $r->{"ext_alt_text"};
    ## Insert
    my $sth = $weak ? $weak_insert_media_sth : $insert_media_sth;
    $sth->bind_param(1, $id, { pg_type => PG_TEXT });
    $sth->bind_param(2, $parent_id, { pg_type => PG_TEXT });
    $sth->bind_param(3, $short_url, { pg_type => PG_TEXT });
    $sth->bind_param(4, $display_url, { pg_type => PG_TEXT });
    $sth->bind_param(5, $media_url, { pg_type => PG_TEXT });
    $sth->bind_param(6, $media_type, { pg_type => PG_TEXT });
    $sth->bind_param(7, $alt_text, { pg_type => PG_TEXT });
    $sth->bind_param(8, $orig, { pg_type => PG_TEXT });
    $sth->bind_param(9, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
}

if ( scalar(@ARGV) != 1 ) {
    die "please run this program with a single argument pointing to the tweets.js file from a Twitter data archive";
}

open my $f, "<", $ARGV[0] or die "can't open $ARGV[0]: $!";

my $part;
my $content;
while ($_ = <$f>) {
    if ( $_ =~ m/^window\.YTD\.tweets\.part(\S+)\s*\=\s*\[\s*/ ) {
	$part = $1;
	$content = "[\n";
	printf STDERR "part %s starts\n", $part;
    } elsif ( $_ =~ m/^\]\s*$/ ) {
	die "bad format" unless defined($part);
	$content .= "]\n";
	my $data = decode_json $content;
	die "unexpected content" unless ref($data) eq "ARRAY";
	printf STDERR "%d tweets found in part %s of archive\n", scalar(@{$data}), $part;
	foreach my $ent ( @{$data} ) {
	    if ( defined($ent->{"tweet"}) ) {
		record_tweet_v1 $ent->{"tweet"}, 0;
	    } else {
		printf STDERR "array item doesn't have key \"tweet\", skipping";
	    }
	}
    } else {
	die "bad format" unless defined($part);
	$content .= $_;
    }
}

close $f;
