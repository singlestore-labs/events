
# Run this perl program on the output from the notifier test to figure out where
# it failed.
#
# Change event/notifer debug = true first

my %unf;
my %fil;
my %threads;
my $done;
my $iteration;

while (<>) {
        $fil{$1} = 1 if /DEBUG: filtered distributor to \S+ starting to \S+ \S+ . (\S+) . \S+/;
        $unf{$1.$2} = 1 if /DEBUG: unfiltered distributor to (\S+) starting to \S+ \S+ . (\S+) . \S+/ ;
        $threads{$1} = 1 if / TEN-((?:unfl|flt)-\d+T-\d+) \S+ (?:sequence-two-S0|waiting)$/;

	$iteration{$1} = $2 if / (TEN-\d+T) \S+ starting iteration (\d+)/;

        $done++ if / TEN-((?:unfl|flt)-\d+T-\d+) \S+ done/;
        delete($threads{$1}) if / TEN-((?:unfl|flt)-\d+T-\d+) \S+ done/;
        delete($fil{$1}) if /DEBUG: filtered distributor to \S+ done handling \S+ . (\S+) . \S+/ ;
        delete($unf{$1.$2}) if /DEBUG: unfiltered distributor to (\S+) done handling \S+ . (\S+) . \S+/ ;
}

foreach my $k (keys(%unf)) {
        print "missing unfiltered ", $k, "\n";
}
foreach my $k (keys(%fil)) {
        print "missing filtered ", $k, "\n";
}
foreach my $k (keys(%threads)) {
        print "unfinished thread ", $k, "\n";
}
foreach my $k (sort keys(%iteration)) {
	print "thread ", $k, " on iteration ", $iteration{$k}, "\n";
}
print "total threads done ", $done, "\n";


