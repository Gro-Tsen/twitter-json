#! /usr/local/bin/perl -w

use strict;
use warnings;

use JSON::XS;
use DateTime::Format::Strptime;

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


sub record_tweet {
    my $r = shift;
    my $id = $r->{"rest_id"};
    unless ( defined($id) && $id =~ m/\A[0-9]+\z/ ) {
	print STDERR "tweet has no id: aborting\n";
	return;
    }
    my $rl = $r->{"legacy"};
    unless ( defined($rl) && ref($rl) eq "HASH" ) {
	print STDERR "tweet $id has no legacy field: aborting\n";
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
    my $html_timestamp = $created_at->strftime("%Y-%m-%dT%H:%M:%S+00:00");
    unless ( defined($r->{"core"}->{"user_results"}->{"result"})
	     && defined($r->{"core"}->{"user_results"}->{"result"}->{"__typename"})
	     && ($r->{"core"}->{"user_results"}->{"result"}->{"__typename"} eq "User")
	     && defined($r->{"core"}->{"user_results"}->{"result"}->{"legacy"}->{"screen_name"}) ) {
	print STDERR "tweet $id has bad or missing user screen_name: aborting\n";
	return;
    }
    my $user_screen_name = $r->{"core"}->{"user_results"}->{"result"}->{"legacy"}->{"screen_name"};
    my $permalink = sprintf("https://twitter.com/%s/status/%s", $user_screen_name, $id);
    my $fulltext = $rl->{"full_text"};
    unless ( defined($fulltext) ) {
	print STDERR "tweet $id has no fulltext: aborting\n";
	return;
    }
    if ( $fulltext =~ m/[\<\>]|\&(?!(?:lt|gt|amp|apos|quot|\#(?:x[0-9A-Fa-f]+|[0-9]+))\;)/ ) {
	print STDERR "tweet $id text contains unescaped HTML: aborting\n";
	return;
    }
    my $fulltext_html = $fulltext;
    my @substitutions = ();
    my @substitutions_html = ();
    if ( defined($rl->{"retweeted_status_result"}) ) {
	unless ( defined($rl->{"retweeted_status_result"}->{"result"})
		 && defined($rl->{"retweeted_status_result"}->{"result"}->{"__typename"})
		 && ($rl->{"retweeted_status_result"}->{"result"}->{"__typename"} eq "Tweet")
		 && defined($rl->{"retweeted_status_result"}->{"result"}->{"rest_id"}) ) {
	    print STDERR "retweet $id has bad or missing retweeted_status_result: aborting\n";
	    return;
	}
	unless ( $fulltext =~ m/\ART\ \@([A-Za-z0-9\_]+)\:/ ) {
	    print STDERR "retweet $id follows bad pattern: aborting\n";
	    return;
	}
	my $rtwdid = $rl->{"retweeted_status_result"}->{"result"}->{"rest_id"};
	my $rtwd = $rl->{"retweeted_status_result"}->{"result"};
	unless ( defined($rtwd->{"core"}->{"user_results"}->{"result"})
		 && defined($rtwd->{"core"}->{"user_results"}->{"result"}->{"__typename"})
		 && ($rtwd->{"core"}->{"user_results"}->{"result"}->{"__typename"} eq "User")
		 && defined($rtwd->{"core"}->{"user_results"}->{"result"}->{"legacy"}->{"screen_name"}) ) {
	    print STDERR "retweeted $rtwdid has bad or missing user screen_name: aborting\n";
	    return;
	}
	my $rt_screen_name = $rtwd->{"core"}->{"user_results"}->{"result"}->{"legacy"}->{"screen_name"};
	push @substitutions_html, [0, 2, "RT", undef, sprintf("<a href=\"https://twitter.com/%s/status/%s\">", $rt_screen_name, $rtwdid), "</a>"];
    }
    for my $ent ( @{$rl->{"entities"}->{"hashtags"}} ) {
	my $idx0 = $ent->{"indices"}->[0];
	my $idx1 = $ent->{"indices"}->[1];
	die "Text contains unescaped HTML" if $ent->{"text"} =~ m/[\<\>]|\&(?!(?:lt|gt|amp|apos|quot|\#(?:x[0-9A-Fa-f]+|[0-9]+))\;)/;
	push @substitutions_html, [$idx0, $idx1-$idx0, "\#".$ent->{"text"}, undef, "<a href=\"https://twitter.com/hashtag/".$ent->{"text"}."\">", "</a>"];
    }
    for my $ent ( @{$rl->{"entities"}->{"user_mentions"}} ) {
	my $idx0 = $ent->{"indices"}->[0];
	my $idx1 = $ent->{"indices"}->[1];
	die "Text contains unescaped HTML" if $ent->{"screen_name"} =~ m/[\<\>]|\&(?!(?:lt|gt|amp|apos|quot|\#(?:x[0-9A-Fa-f]+|[0-9]+))\;)/;
	push @substitutions_html, [$idx0, $idx1-$idx0, undef, undef, "<a href=\"https://twitter.com/".$ent->{"screen_name"}."\">", "</a>"];
    }
    for my $ent ( @{$rl->{"entities"}->{"urls"}} ) {
	my $idx0 = $ent->{"indices"}->[0];
	my $idx1 = $ent->{"indices"}->[1];
	push @substitutions, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"expanded_url"})];
	push @substitutions_html, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"display_url"}), "<a href=\"".html_quote($ent->{"expanded_url"})."\">", "</a>"];
    }
    # for my $ent ( @{$rl->{"entities"}->{"media"}} ) {
    # 	my $idx0 = $ent->{"indices"}->[0];
    # 	my $idx1 = $ent->{"indices"}->[1];
    # 	push @substitutions, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"display_url"})];
    # 	push @substitutions_html, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"display_url"}), "<a href=\"".html_quote($ent->{"expanded_url"})."\">", "</a>"];
    # }
    $fulltext = substitute_in_string $fulltext, \@substitutions;
    die "fulltext contains unescaped HTML: $fulltext" if $fulltext =~ m/[\<\>]|\&(?!(?:lt|gt|amp|apos|quot|\#(?:x[0-9A-Fa-f]+|[0-9]+))\;)/;
    $fulltext = html_unquote $fulltext;
    $fulltext_html = substitute_in_string $fulltext_html, \@substitutions_html;
    die "HTML fulltext contains unescaped HTML: $fulltext_html" if $fulltext_html =~ m/\&(?!(?:lt|gt|amp|apos|quot|\#(?:x[0-9A-Fa-f]+|[0-9]+))\;)/;
    $fulltext_html =~ s/\n/\<span class=\"br\"\>\&#x2424;\<\/span\>/g;
    # HTML-escape astral characters
    $fulltext_html =~ s/([^\x{0020}-\x{ffff}])/sprintf("\&\#x%x\;",ord($1))/ge;
    # Now save HTML archive in a hash
    my $userlink = "";
    # my $userlink = sprintf(" <a href=\"%s\">\@%s</a>", "https://twitter.com/".$user_screen_name, $user_screen_name);
    my $lang = $rl->{"lang"};
    my $langattr = defined($lang) && $lang ne "und" ? " xml:lang=\"$lang\"" : "";
    my $replying = "";
    if ( defined($rl->{"in_reply_to_status_id_str"}) ) {
	$replying = sprintf " <a href=\"https://twitter.com/%s/status/%s\">\x{2709}</a>", $rl->{"in_reply_to_screen_name"}, $rl->{"in_reply_to_status_id_str"};
    }
    printf "\<dt id=\"tweet-%s\"%s\><a href=\"%s\"><time>%s</time></a>%s%s</dt><dd>%s</dd>\n", $id, $langattr, $permalink, $html_timestamp, $userlink, $replying, $fulltext_html;
}

sub process_tweet {
    my $r = shift;
    my $id = $r->{"rest_id"};
    if ( !defined($id) || !defined($r->{"legacy"})
	 || !(ref($r->{"legacy"}) eq "HASH")
	 || !defined($r->{"legacy"}->{"id_str"})
	 || !($r->{"legacy"}->{"id_str"} eq $id) ) {
	printf STDERR "warning: basic sanity checks failed for tweet %s\n", ($id//"(unknown)");
	return;
    }
    if ( !defined($r->{"core"})
	 || !(ref($r->{"core"}) eq "HASH") ) {
	printf STDERR "warning: extra sanity checks failed for tweet %s\n", ($id//"(unknown)");
    }
    record_tweet $r;
    # Recurse on the retweeted or quoted status?
    if ( defined($r->{"legacy"}->{"retweeted_status_result"}) ) {
	if ( !(ref($r->{"legacy"}->{"retweeted_status_result"}) eq "HASH")
	     || !defined($r->{"legacy"}->{"retweeted_status_result"}->{"result"})
	     || !(ref($r->{"legacy"}->{"retweeted_status_result"}->{"result"}) eq "HASH") ) {
	    printf STDERR "warning: retweeted_status_result sanity checks failed for tweet %s\n", ($id//"(unknown)");
	} else {
	    recurse ($r->{"legacy"}->{"retweeted_status_result"});
	}
    }
    if ( defined($r->{"quoted_status_result"}) ) {
	if ( !(ref($r->{"quoted_status_result"}) eq "HASH")
	     || !defined($r->{"quoted_status_result"}->{"result"})
	     || !(ref($r->{"quoted_status_result"}->{"result"}) eq "HASH") ) {
	    printf STDERR "warning: quoted_status_result sanity checks failed for tweet %s\n", ($id//"(unknown)");
	} else {
	    recurse ($r->{"quoted_status_result"});
	}
    }
}

sub recurse {
    my $r = shift;
    if ( ref($r) eq "ARRAY" ) {
	for ( my $i=0 ; $i<scalar(@{$r}) ; $i++ ) {
	    recurse ($r->[$i]);
	}
    } elsif ( ref($r) eq "HASH" ) {
	if ( defined($r->{"__typename"}) && $r->{"__typename"} eq "Tweet" ) {
	    process_tweet($r);
	    return;  # Recusion is done inside process_tweet
	}
	foreach my $k ( keys(%{$r}) ) {
	    recurse ($r->{$k});
	}
    }
}

my $content;
if (1) {
    local $/ = undef;  # enable localized slurp mode
    $content = <>;
}
my $data = decode_json $content;

recurse $data;
