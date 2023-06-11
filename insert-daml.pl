#! /usr/local/bin/perl -w

# Do not use this program if you're not David Madore, it won't be of
# any use to you.

use strict;
use warnings;

use Getopt::Std;

use POSIX qw(strftime);

use JSON::XS;
use DateTime::Format::Strptime;

use DBI qw(:sql_types);
use DBD::Pg qw(:pg_types);

my %opts;
getopts('d:ws:', \%opts);

my $global_weak = $opts{w};
my $global_source = $opts{s} // "daml-archive";

my $global_user_id = $ENV{"TWEETS_ARCHIVE_USER_ID"};
my $global_user_screen_name = $ENV{"TWEETS_ARCHIVE_USER_SCREEN_NAME"};
unless ( defined($global_user_id) && defined($global_user_screen_name) ) {
    die "please run this program with TWEETS_ARCHIVE_USER_ID and TWEETS_ARCHIVE_USER_SCREEN_NAME evironment variables set";
}

my $json_coder_unicode = JSON::XS->new;
my $json_decoder_unicode = JSON::XS->new;
my $json_coder_utf8 = JSON::XS->new->utf8;
my $json_decoder_utf8 = JSON::XS->new->utf8;

binmode STDOUT, ":utf8";
binmode STDERR, ":utf8";

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

my $html_datetime_parser = DateTime::Format::Strptime->new(
    pattern => "%Y-%m-%dT%H:%M:%S%z", time_zone => "UTC", locale => "C");

my $dbname = $opts{d} // "twitter";
my $dbh;  # Connection to database
my $insert_authority_sth;
my $weak_insert_authority_sth;
my $insert_tweet_sth;
my $weak_insert_tweet_sth;

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
	. ", meta_updated_at = GREATEST(EXCLUDED.meta_updated_at, tweets.meta_updated_at) "
	. ", meta_source = CASE WHEN EXCLUDED.meta_updated_at > tweets.meta_updated_at THEN EXCLUDED.meta_source ELSE tweets.meta_source END ";
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
	. ", meta_updated_at = GREATEST(EXCLUDED.meta_updated_at, tweets.meta_updated_at) "
	. ", meta_source = CASE WHEN EXCLUDED.meta_updated_at > tweets.meta_updated_at THEN EXCLUDED.meta_source ELSE tweets.meta_source END ";
    $returning = "RETURNING id , meta_updated_at";
    $insert_tweet_sth = $dbh->prepare($command . $conflict . $returning);
    $weak_insert_tweet_sth = $dbh->prepare($command . $weak_conflict . $returning);
}

do_connect;

my $global_auth_source;
my $global_auth_date;

my $global_tweet_count = 0;

sub record_tweet_daml {
    # Insert tweet into database.  Arguments are the tweet's HTML
    # line, and a weak parameter indicating whether we should leave
    # existing entries.
    my $line = shift;
    my $weak = shift || $global_weak;  # If 1 leave existing records be
    unless ( $line =~ m/^\<dt\s+id\=\"tweet-([0-9]+)\"([^\<\>]*)\>\<a\s+href\=\"([^\"\<\>]*)\"\>\<time\>([^\<\>]*)\<\/time\>\<\/a\>(?:\s+\<a\s+href\=\"([^\<\>]*)\"\>\x{2709}\<\/a\>)?\<\/dt\>\<dd\>(.*)\<\/dd\>$/ ) {
	print STDERR "the following tweet line is badly formed: aborting\n";
	print STDERR "line: $line";
	return;
    }
    my $r = {};
    my $id = $1;
    my $attrlist = $2;
    my $permalink = $3;
    my $html_timestamp = $4;
    my $replylink = $5;
    my $full_text_html = $6;
    my $author_id = $global_user_id;
    my $author_screen_name = $global_user_screen_name;
    my $lang;
    if ( $attrlist =~ m/\s+xml\:lang\=\"([A-Za-z0-9\-]+)\"/ ) {
	$lang = $1;
    }
    unless ( $permalink =~ m/\Ahttps\:\/\/twitter\.com\/([A-Za-z0-9\_]+)\/status\/([0-9]+)\z/
	&& $2 eq $id ) {
	print STDERR "the following tweet line has badly formed permalink: aborting\n";
	print STDERR "line: $line";
	return;
    }
    my $created_at = $html_datetime_parser->parse_datetime($html_timestamp);
    unless ( defined($created_at) ) {
	print STDERR "the following tweet line has invalid creation date: aborting\n";
	print STDERR "line: $line";
	return;
    }
    my $created_at_twitter = $created_at->strftime("%a %b %d %T %z %Y");
    my $created_at_iso = $created_at->strftime("%Y-%m-%d %H:%M:%S%z");
    my ($replyto_id, $replyto_author_screen_name);
    if ( defined($replylink) ) {
	unless ( $replylink =~ m/\Ahttps\:\/\/twitter\.com\/([A-Za-z0-9\_]+)\/status\/([0-9]+)\z/ ) {
	    print STDERR "the following tweet line has badly formed reply link: aborting\n";
	    print STDERR "line: $line";
	    return;
	}
	$replyto_id = $2;
	$replyto_author_screen_name = $1;
    }
    my $full_text = "";
    my $input_text = "";
    my ($retweeted_id, $retweeted_author_screen_name);
    my ($quoted_id, $quoted_author_screen_name);
    if ( $full_text_html =~ s/\A\<a\s+href\=\"([^\<\>]*)\"\>RT\<\/a\>/RT/ ) {
	my $retweetlink = $1;
	unless ( $retweetlink =~ m/\Ahttps\:\/\/twitter\.com\/([A-Za-z0-9\_]+)\/status\/([0-9]+)\z/ ) {
	    print STDERR "the following tweet line has badly formed retweet link: aborting\n";
	    print STDERR "line: $line";
	    return;
	}
	$retweeted_id = $2;
	$retweeted_author_screen_name = $1;
    }
    my $started_link_target;
    my $started_link_position;
    my $started_link_position_inputtext;
    my @hashtags;
    my @user_mentions;
    my @urls;
    my @media;
    my $quoted_permalink_r;
    while ( 1 ) {
	if ( $full_text_html =~ s/\A([^\<\>\&]+)// ) {
	    $full_text .= $1;
	    $input_text .= $1;
	} elsif ( $full_text_html =~ s/\A(\&(?:amp|lt|gt|quot)\;)// ) {
	    $full_text .= $1;
	    $input_text .= $1;
	} elsif ( $full_text_html =~ s/\A\&\#[Xx]([0-9A-Fa-f]+)\;// ) {
	    my $val = hex($1);
	    if ( $val < 0x20 || $val == 0x26 || $val == 0x3c
		 || $val == 0x3e || $val == 0x22 ) {
		print STDERR "the following tweet line uses the wrong HTML escape: aborting\n";
		print STDERR "line: $line";
		return;
	    }
	    $full_text .= chr($val);
	    $input_text .= chr($val);
	} elsif ( $full_text_html =~ s/\A\<span\s+class=\"br\"\>\&#x2424\;\<\/span\>// ) {
	    $full_text .= "\n";
	    $input_text .= "\n";
	} elsif ( $full_text_html =~ s/\A\<a\s+href\=\"([^\<\>]*)\"\>// ) {
	    if ( defined($started_link_target) ) {
		print STDERR "the following tweet line attempts to nest links: aborting\n";
		print STDERR "line: $line";
		return;
	    }
	    $started_link_target = $1;
	    $started_link_position = length($full_text);
	    $started_link_position_inputtext = length($input_text);
	} elsif ( $full_text_html =~ s/\A\<\/a\>// ) {
	    unless ( defined($started_link_position) ) {
		print STDERR "the following tweet line attempts to close an unclosed link: aborting\n";
		print STDERR "line: $line";
		return;
	    }
	    my $entity_text = substr($full_text, $started_link_position);
	    if ( $entity_text =~ m/\A\#/ ) {
		# Hashtag
		push @hashtags, { "indices" => [$started_link_position,length($full_text)], "text" => substr($entity_text,1) };
	    } elsif ( $entity_text =~ m/\A\@/ ) {
		# User mention
		push @user_mentions, { "indices" => [$started_link_position,length($full_text)], "screen_name" => substr($entity_text,1) };
	    } elsif ( $entity_text =~ m/\Apic\.twitter\.com\/(\S+)\z/ ) {
		# Media link
		my $sfx = $1;
		my $fake_url = "https://t.co/$sfx";
		push @media, { "indices" => [$started_link_position,$started_link_position+length($fake_url)], "url" => $fake_url, "display_url" => $entity_text, "expanded_url" => html_unquote($started_link_target) };
		substr($full_text, $started_link_position) = $fake_url;
	    } else {
		my $fake_url = "org-madore-twitter-link://" . $id . "/url/" . (scalar(@urls));
		if ( $started_link_target =~ m/\Ahttps\:\/\/twitter\.com\/([A-Za-z0-9\_]+)\/status\/([0-9]+)\z/ ) {
		    $quoted_id = $2;
		    $quoted_author_screen_name = $1;
		    $fake_url = $started_link_target;
		    $quoted_permalink_r = {};
		    $quoted_permalink_r->{"display"} = $entity_text;
		    $quoted_permalink_r->{"expanded"} = $started_link_target;
		    $quoted_permalink_r->{"url"} = $fake_url;
		}
		push @urls, { "indices" => [$started_link_position,$started_link_position+length($fake_url)], "url" => $fake_url, "display_url" => $entity_text, "expanded_url" => html_unquote($started_link_target) };
		substr($full_text, $started_link_position) = $fake_url;
		substr($input_text, $started_link_position_inputtext) = $started_link_target;
	    }
	    $started_link_target = undef;
	    $started_link_position = undef;
	    $started_link_position_inputtext = undef;
	} elsif ( $full_text_html eq "" ) {
	    last;
	} else {
	    print STDERR "the following tweet line contains unparsable text: aborting\n";
	    print STDERR "line: $line";
	    print STDERR "unparsable from: $full_text_html\n";
	    return;
	}
    }
    $input_text = html_unquote($input_text);
    $r->{"id_str"} = $id;
    $r->{"created_at"} = $created_at_twitter;
    $r->{"user_id_str"} = $author_id;
    $r->{"user_screen_name"} = $author_screen_name;
    $r->{"in_reply_to_status_id_str"} = $replyto_id if defined($replyto_id);
    $r->{"in_reply_to_screen_name"} = $replyto_author_screen_name if defined($replyto_author_screen_name);
    $r->{"retweeted_status_id_str"} = $retweeted_id if defined($retweeted_id);
    $r->{"retweeted_screen_name"} = $retweeted_author_screen_name if defined($retweeted_author_screen_name);
    $r->{"quoted_status_id_str"} = $quoted_id if defined($quoted_id);
    $r->{"quoted_screen_name"} = $quoted_author_screen_name if defined($quoted_author_screen_name);
    $r->{"quoted_status_permalink"} = $quoted_permalink_r if defined($quoted_permalink_r);
    $r->{"full_text"} = $full_text;
    $r->{"entities"} = {
	"hashtags" => \@hashtags,
	"user_mentions" => \@user_mentions,
	"media" => \@media,
	"urls" => \@urls
    };
    $r->{"lang"} = $lang;
    my $rl = $r;  # Simplify synchronization with insert-json.pl
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
    $sth->bind_param(5, undef, { pg_type => PG_TEXT });
    $sth->bind_param(6, undef, { pg_type => PG_TEXT });
    $sth->bind_param(7, $replyto_id, { pg_type => PG_TEXT });
    $sth->bind_param(8, undef, { pg_type => PG_TEXT });
    $sth->bind_param(9, $replyto_author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(10, $retweeted_id, { pg_type => PG_TEXT });
    $sth->bind_param(11, undef, { pg_type => PG_TEXT });
    $sth->bind_param(12, $retweeted_author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(13, $quoted_id, { pg_type => PG_TEXT });
    $sth->bind_param(14, undef, { pg_type => PG_TEXT });
    $sth->bind_param(15, $quoted_author_screen_name, { pg_type => PG_TEXT });
    $sth->bind_param(16, $full_text, { pg_type => PG_TEXT });
    $sth->bind_param(17, $input_text, { pg_type => PG_TEXT });
    $sth->bind_param(18, $lang, { pg_type => PG_TEXT });
    $sth->bind_param(19, undef, SQL_INTEGER);
    $sth->bind_param(20, undef, SQL_INTEGER);
    $sth->bind_param(21, undef, SQL_INTEGER);
    $sth->bind_param(22, undef, SQL_INTEGER);
    $sth->bind_param(23, $meta_date, { pg_type => PG_TIMESTAMPTZ });
    $sth->bind_param(24, $global_source, { pg_type => PG_TEXT });
    $sth->execute();
    $ret = $sth->fetchall_arrayref;
    die "insertion into database failed" unless defined($ret->[0][0]) && ($ret->[0][0] eq $id);
    $dbh->commit;
    $global_tweet_count++;
}

sub process_line {
    my $line = shift;
    if ( $line =~ m/\<dt\ id\=\"tweet-[0-9]+\"/ ) {
	record_tweet_daml $line;
    }
}

if ( scalar(@ARGV) ) {
    foreach my $fname ( @ARGV ) {
	open my $f, "<:utf8", $fname
	    or die "can't open $fname: $!";
	print STDERR "processing file $fname\n";
	$global_auth_source = $fname;
	$global_auth_date = strftime("%Y-%m-%d %H:%M:%S+00:00", gmtime((stat($f))[9]));
	while ($_ = <$f>) {
	    process_line $_;
	}
	close $f;
    }
} else {
    binmode STDIN, ":utf8";
    $global_auth_date = strftime("%Y-%m-%d %H:%M:%S+00:00", gmtime((stat(STDIN))[9]));
    while ($_ = <STDIN>) {
	process_line $_;
    }
}

print "inserted or updated: $global_tweet_count tweets\n";
