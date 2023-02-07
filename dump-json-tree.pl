#! /usr/local/bin/perl -w

use strict;
use warnings;

use JSON::XS;

binmode STDOUT, ":utf8";

my $content;
if (1) {
    local $/ = undef;  # enable localized slurp mode
    $content = <>;
}
my $data = decode_json $content;

sub quotestring {
    my $str = shift;
    my $q = $str;
    $q =~ s/([\"\\])/\\$1/g;
    $q =~ s/\n/\\n/g;
    $q = "\"" . $q . "\"";
    return $q;
}

sub recurse {
    my $r = shift;
    my $path = shift;
    if ( ref($r) eq "ARRAY" ) {
	for ( my $i=0 ; $i<scalar(@{$r}) ; $i++ ) {
	    recurse ($r->[$i], ($path . "[" . $i . "]"));
	}
    } elsif ( ref($r) eq "HASH" ) {
	foreach my $k ( keys(%{$r}) ) {
	    recurse ($r->{$k}, ($path . "[" . quotestring($k) . "]"));
	}
    } elsif ( ref($r) eq "" ) {
	print "$path" . "=" . quotestring($r) . ";\n\n";
    }
}

recurse $data, "";
