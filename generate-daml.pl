#! /usr/local/bin/perl -w

use strict;
use warnings;

use Getopt::Std;

use POSIX qw(strftime);

use JSON::XS;
use DateTime::Format::Strptime;

my %opts;
getopts('ad:hw', \%opts);

my $har_mode = $opts{h};

my $record_all = $opts{a};

my $global_user_id = $ENV{"TWEETS_ARCHIVE_USER_ID"};
unless ( defined($global_user_id) ) {
    die "please run this program with TWEETS_ARCHIVE_USER_ID evironment variables set";
}

my $json_decoder_unicode = JSON::XS->new;
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



# Read existing HTML archive
print STDERR "Reading HTML dump...\n";
my $htmldumpfile = $opts{d} // "tweets.daml";
my $html_preface = "";  my $html_postface = "";
my %html_dump;
if ( ! open my $htmldumpf, "<:utf8", $htmldumpfile ) {
    $html_preface = "\<\!-- \@\@BEGIN\@\@ --\>\n";
    $html_postface = "\<\!-- \@\@END\@\@ --\>\n";
} else {
    my $state = 0;
    while (<$htmldumpf>) {
	if ( $state == 0 ) {
	    $html_preface .= $_;
	    if ($_ =~ m/\<\!\-\- \@\@BEGIN\@\@ \-\-\>/) {
		$state = 1;
	    }
	} elsif ( $state == 1 ) {
	    if ($_ =~ m/\<\!\-\- \@\@END\@\@ \-\-\>/) {
		$state = 2;
		$html_postface .= $_;
	    } else {
		$html_dump{$1} = $_ if $_ =~ m/^\<dt id\=\"tweet-([0-9]+)\"/;
		# Simply ignore and delete any other line!
	    }
	} elsif ( $state == 2 ) {
	    $html_postface .= $_;
	}
    }
    close $htmldumpf;
}



my $global_tweet_count = 0;

sub record_tweet {
    # Insert tweet into file.  Arguments are the ref to the
    # tweet's (decoded) JSON.
    my $r = shift;
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
    my $created_at_html = $created_at->strftime("%Y-%m-%dT%H:%M:%S+00:00");
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
	print STDERR "tweet $id has no author screen name\n";
    }
    my $conversation_id = $rl->{"conversation_id_str"};
    my $thread_id = $rl->{"self_thread"}->{"id_str"};
    my $permalink = sprintf("https://twitter.com/%s/status/%s", $author_screen_name, $id);
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
	if ( defined($rtwd)
	     && defined($rtwd->{"__typename"})
	     && ( $rtwd->{"__typename"} eq "TweetWithVisibilityResults" )
	     && defined($rtwd->{"tweet"}) ) {
	    # TweetWithVisibilityResults objects are used to limit
	    # replies.  We just skip to the "tweet" content, and fake
	    # its __typename.
	    $rtwd = $rtwd->{"tweet"};
	    $rtwd->{"__typename"} = "Tweet";  # Fake it!
	}
	unless ( defined($rtwd)
		 && defined($rtwd->{"__typename"})
		 && ( $rtwd->{"__typename"} eq "Tweet" )
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
	$retweeted_author_screen_name = $rtwd->{"core"}->{"user_results"}->{"result"}->{"legacy"}->{"screen_name"};
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
    my $quoted_permalink_display = $rl->{"quoted_status_permalink"}->{"display"};
    if ( defined($quoted_id) ) {QUOTED_IF:{
	my $qtwd = $r->{"quoted_status_result"}->{"result"};
	# if ( defined($qtwd)
	#      && defined($qtwd->{"__typename"})
	#      && ($qtwd->{"__typename"} eq "TweetTombstone") ) {
	#     last QUOTED_IF;
	# }
	if ( defined($qtwd)
	     && defined($qtwd->{"__typename"})
	     && ( $qtwd->{"__typename"} eq "TweetWithVisibilityResults" )
	     && defined($qtwd->{"tweet"}) ) {
	    # TweetWithVisibilityResults objects are used to limit
	    # replies.  We just skip to the "tweet" content, and fake
	    # its __typename.
	    $qtwd = $qtwd->{"tweet"};
	    $qtwd->{"__typename"} = "Tweet";  # Fake it!
	}
	unless ( defined($qtwd)
		 && defined($qtwd->{"__typename"})
		 && ( $qtwd->{"__typename"} eq "Tweet" ) ) {
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
	$quoted_author_screen_name = $qtwd->{"core"}->{"user_results"}->{"result"}->{"legacy"}->{"screen_name"};
	unless ( defined($quoted_author_screen_name) ) {
	    print STDERR "tweet $id quoting $quoted_id gives no author screen name\n";
	    last QUOTED_IF;
	}
    }}
    ## Text
    my $full_text = $rl->{"full_text"};
    my $media_lst_r = ($rl->{"extended_entities"}->{"media"}) // ($rl->{"entities"}->{"media"});
    unless ( defined($full_text) ) {
	print STDERR "tweet $id has no text: aborting\n";
	return;
    }
    # Attempt to reconstruct tweet input text
    my $input_text = $full_text;
    my $html_text = $full_text;
  SUBSTITUTE:{
      if ( $input_text =~ m/[\<\>]|\&(?!lt\;|gt\;|amp\;|apos\;|quot\;)/ ) {
	  # Old tweets may have unescaped HTML.  Leave them alone!
	  print STDERR "tweet $id contains unescaped HTML\n"
	      if $created_at_iso ge "2018";
	  last SUBSTITUTE;
      }
      my @substitutions;
      my @substitutions_html;
      my $found_quoted_permalink_in_entities = 0;
      if ( defined($retweeted_id) ) {SUB_RETWETED_IF:{
	  unless ( $html_text =~ m/\ART\ \@([A-Za-z0-9\_]+)\:/
# This seems to barf on harmless name changes
##		   && $1 eq $retweeted_author_screen_name
	      ) {
	      print STDERR "tweet $id retweeting $retweeted_id follows bad pattern\n";
	      last SUB_RETWEETED_IF;
	  }
	  push @substitutions_html, [0, 2, "RT", undef, sprintf("<a href=\"https://twitter.com/%s/status/%s\">", $retweeted_author_screen_name, $retweeted_id), "</a>"];
      }}
      unless ( defined($rl->{"entities"}) ) {
	  print STDERR "tweet $id has no entities part\n";
	  last SUBSTITUTE;
      }
      if ( defined($rl->{"entities"}->{"hashtags"}) ) {
	  for my $ent ( @{$rl->{"entities"}->{"hashtags"}} ) {
	      my $idx0 = $ent->{"indices"}->[0];
	      my $idx1 = $ent->{"indices"}->[1];
	      if ( $ent->{"text"} =~ m/[\<\>]|\&(?!lt\;|gt\;|amp\;|apos\;|quot\;)/ ) {
		  print STDERR "tweet $id: hashtag contains unescaped HTML\n";
		  last SUBSTITUTE;
	      }
	      push @substitutions_html, [$idx0, $idx1-$idx0, "\#".$ent->{"text"}, undef, "<a href=\"https://twitter.com/hashtag/".$ent->{"text"}."\">", "</a>"];
	  }
      }
      if ( defined($rl->{"entities"}->{"user_mentions"}) ) {
	  for my $ent ( @{$rl->{"entities"}->{"user_mentions"}} ) {
	      my $idx0 = $ent->{"indices"}->[0];
	      my $idx1 = $ent->{"indices"}->[1];
	      if ( $ent->{"screen_name"} =~ m/[\<\>]|\&(?!lt\;|gt\;|amp\;|apos\;|quot\;)/ ) {
		  print STDERR "tweet $id: hashtag contains unescaped HTML\n";
		  last SUBSTITUTE;
	      }
	      push @substitutions_html, [$idx0, $idx1-$idx0, undef, undef, "<a href=\"https://twitter.com/".$ent->{"screen_name"}."\">", "</a>"];
	  }
      }
      if ( defined($rl->{"entities"}->{"urls"}) ) {
	  for my $ent ( @{$rl->{"entities"}->{"urls"}} ) {
	      my $idx0 = $ent->{"indices"}->[0];
	      my $idx1 = $ent->{"indices"}->[1];
	      unless ( defined($ent->{"expanded_url"}) ) {
		  print STDERR "substitution missing expanded_url: skipping\n";
		  next;
	      }
	      push @substitutions, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"expanded_url"})];
	      push @substitutions_html, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"display_url"}), "<a href=\"".html_quote($ent->{"expanded_url"})."\">", "</a>"];
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
	      unless ( defined($ent->{"expanded_url"}) ) {
		  print STDERR "substitution missing expanded_url: skipping\n";
		  next;
	      }
	      # Multiple media entities may subtitute the same part of
	      # the tweet: do this only once.
	      unless ( defined($previdx0) && $idx0==$previdx0 ) {
		  push @substitutions, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"display_url"})];
		  push @substitutions_html, [$idx0, $idx1-$idx0, $ent->{"url"}, html_quote($ent->{"display_url"}), "<a href=\"".html_quote($ent->{"expanded_url"})."\">", "</a>"];
	      }
	      $previdx0 = $idx0;
	  }
      }
      $input_text = substitute_in_string $input_text, \@substitutions;
      $html_text = substitute_in_string $html_text, \@substitutions_html;
      # If the permalink of the quoted tweet was not found in the URLs
      # being substituted, add it explicitly at the end.
      if ( defined($quoted_permalink)
	   && ! $found_quoted_permalink_in_entities ) {
	  # (Note that permalink can contain unescaped '&' here.)
	  $input_text .= (($input_text=~m/\s\z/)?"":" ") . (html_quote($quoted_permalink));
	  $html_text .= (($html_text=~m/\s\z/)?"":" ") . "<a href=\"".html_quote($quoted_permalink)."\">" . (html_quote($quoted_permalink_display)) . "</a>";
      }
      $input_text = html_unquote $input_text;
      $html_text =~ s/\n/\<span class=\"br\"\>\&#x2424;\<\/span\>/g;
      # HTML-escape astral characters
      $html_text =~ s/([^\x{0020}-\x{ffff}])/sprintf("\&\#x%x\;",ord($1))/ge;
    }
    ## Miscellaneous
    my $lang = $rl->{"lang"};
    my $favorite_count = $rl->{"favorite_count"};
    my $retweet_count = $rl->{"retweet_count"};
    my $quote_count = $rl->{"quote_count"};
    my $reply_count = $rl->{"reply_count"};
    ## Create HTML line
    unless ( $record_all || ( $author_id eq $global_user_id ) ) {
	return;
    }
    my $html_userlink = $author_id eq $global_user_id ? "" : sprintf(" <a href=\"%s\">\@%s</a>", "https://twitter.com/".$author_screen_name, $author_screen_name);
    my $html_langattr = defined($lang) && $lang ne "und" ? " xml:lang=\"$lang\"" : "";
    my $html_replying = "";
    if ( defined($replyto_id) ) {
	$html_replying = sprintf " <a href=\"https://twitter.com/%s/status/%s\">\x{2709}</a>", $replyto_author_screen_name // "_", $replyto_id;
    }
    $html_dump{$id} = sprintf "\<dt id=\"tweet-%s\"%s\><a href=\"%s\"><time>%s</time></a>%s%s</dt><dd>%s</dd>\n", $id, $html_langattr, $permalink, $created_at_html, $html_userlink, $html_replying, $html_text;
    $global_tweet_count++;
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
	if ( defined($r->{"__typename"})
	     && ( $r->{"__typename"} eq "Tweet" )
	     && defined($r->{"rest_id"}) ) {
	    record_tweet($r, 0);
	    return;  # Recusion is done inside record_tweet
	} elsif ( defined($r->{"__typename"})
	     && ( $r->{"__typename"} eq "TweetWithVisibilityResults" )
	     && defined($r->{"tweet"}) ) {
	    record_tweet($r->{"tweet"}, 0);
	    return;  # Recusion is done inside record_tweet
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


open my $htmldumpf, ">:utf8", $htmldumpfile or die "Failed to open $htmldumpfile: $!";
print $htmldumpf $html_preface;
print $htmldumpf "<dl>\n";
foreach my $id ( sort {$b cmp $a} keys(%html_dump) ) {
    print $htmldumpf $html_dump{$id};
}
print $htmldumpf "</dl>\n";
print $htmldumpf $html_postface;
close $htmldumpf;


print STDERR "inserted or updated: $global_tweet_count tweets\n";
