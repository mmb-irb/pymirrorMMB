#!/usr/bin/perl
#
# @File Log.pm
# @Author gelpi
# @Created 06-jun-2016 14:30:45
#

package Log;

use strict;
use Time::localtime;
use Exporter;
our $VERSION = 1.0;
our @ISA = qw(Exporter);
our @EXPORT = qw(printLog);
our @EXPORT_OK = ();

sub printLog {
    my $str = shift;
    my $dateStr = _dateStr();
    foreach my $lin (split /\n/,$str) {
        print $dateStr." $lin\n";
    }
        
}

sub printProgress {
    my ($prefix, $n, $tot, $inc) =@_;
    if (!$inc) {
        $inc='20%'
    }
    if ($inc =~ /%$/) {
        $inc = $tot/100.*$inc;
    }
    if (int($n/$inc) == $n/$inc) {
        printLog (sprintf "%s %d/%d %5.0f%",$prefix, $n, $tot, ($n*100./$tot));
    }
    
}

sub _dateStr  {
    return sprintf "[%4d-%02d-%02d|%02d:%02d:%02d]",(localtime->year()+1900),1+localtime->mon(),localtime->mday(),localtime->hour(),localtime->min(),localtime->sec();
}

1;
