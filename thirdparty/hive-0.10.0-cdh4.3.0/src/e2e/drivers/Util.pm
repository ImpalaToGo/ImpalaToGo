#!/usr/bin/env perl 

############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
 

###########################################################################
# Class: Util
#
# A collection of  helper subroutines.
#


package Util;

use IPC::Run qw(run);
use strict;

##############################################################################
#  Sub: setupHiveProperties
#
#  Assure that necessary values are set in config in order to set Hive
#  Java properties.
# 
#  Returns:
#  Nothing
sub  setupHiveProperties($$)
{
    my ($cfg, $log) = @_;

    # Set up values for the metastore
    if (defined($cfg->{'metastore_thrift'}) && $cfg->{'metastore_thrift'} == 1) {
        if (! defined $cfg->{'metastore_host'} || $cfg->{'metastore_host'} eq "") {
            print $log "When using thrift, you must set the key " .
                " 'metastore_host' to the machine your metastore is on\n";
            die "metastore_host is not set in existing.conf\n";
        }

        $cfg->{'metastore_connection'} =
            "jdbc:$cfg->{'metastore_db'}://$cfg->{'metastore_host'}/hivemetastoredb?createDatabaseIfNotExist=true";
    
        if (! defined $cfg->{'metastore_passwd'} || $cfg->{'metastore_passwd'} eq "") {
            $cfg->{'metastore_passwd'} = 'hive';
        }

        if (! defined $cfg->{'metastore_port'} || $cfg->{'metastore_port'} eq "") {
            $cfg->{'metastore_port'} = '9933';
        }

        $cfg->{'metastore_uri'} =
            "thrift://$cfg->{'metastore_host'}:$cfg->{'metastore_port'}";
    } else {
        $cfg->{'metastore_connection'} =
            "jdbc:derby:;databaseName=metastore_db;create=true";
        $cfg->{'metastore_driver'} = "org.apache.derby.jdbc.EmbeddedDriver";
    }
}

##############################################################################
#  Sub: runHiveCmdFromFile
#
#  Run the provided file using the Hive command line.
# 
#  cfg - The configuration file for the test
#  log - reference to the log file, should be an open file pointer
#  sql - name of file containing SQL to run.  Optional, if present -f $sql
#    will be appended to the command.
#  outfile - open file pointer (or variable reference) to write stdout to for
#    this test.  Optional, will be written to $log if this value is not
#    provided.
#  outfile - open file pointer (or variable reference) to write stderr to for
#    this test.  Optional, will be written to $log if this value is not
#    provided.
#  noFailOnFail - if true, do not fail when the Hive command returns non-zero
#    value.
#  Returns:
#  Nothing
sub runHiveCmdFromFile($$;$$$$)
{
    my ($cfg, $log, $sql, $outfile, $errfile, $noFailOnFail) = @_;

    if (!defined($ENV{'HADOOP_HOME'})) {
        die "Cannot run hive when HADOOP_HOME environment variable is not set.";
    }

    $outfile = $log if (!defined($outfile));
    $errfile = $log if (!defined($errfile));

    my @cmd;
    if (defined($sql)) {
        @cmd = ("$cfg->{'hivehome'}/bin/hive", "-f", $sql);
    } else {
        @cmd = ("$cfg->{'hivehome'}/bin/hive");
    }

    # Add all of the modified properties we want to set
    push(@cmd,
        ("--hiveconf", "javax.jdo.option.ConnectionURL=$cfg->{'metastore_connection'}",
         "--hiveconf", "javax.jdo.option.ConnectionDriverName=$cfg->{'metastore_driver'}"));

    if (defined($cfg->{'metastore_thrift'}) && $cfg->{'metastore_thrift'} == 1) {
        push(@cmd,
            ("--hiveconf", "hive.metastore.local=false",
             "--hiveconf", "hive.metastore.uris=thrift://$cfg->{'metastore_host'}:$cfg->{'metastore_port'}",
             "--hiveconf", "javax.jdo.option.ConnectionPassword=$cfg->{'metastore_passwd'}"));
    }

    if (defined($cfg->{'additionaljarspath'})) {
        $ENV{'HIVE_AUX_JARS_PATH'} = $cfg->{'additionaljarspath'};
    }

    if (defined($cfg->{'hiveconf'})) {
        foreach my $hc (@{$cfg->{'hiveconf'}}) {
            push(@cmd, "--hiveconf", $hc);
        }
    }

    if (defined($cfg->{'hivecmdargs'})) {
        push(@cmd, @{$cfg->{'hivecmdargs'}});
    }

    if (defined($cfg->{'hiveops'})) {
        $ENV{'HIVE_OPTS'} = join(" ", @{$cfg->{'hiveops'}});
    }

    $ENV{'HIVE_HOME'} = $cfg->{'hivehome'};

    my $envStr;
    for my $k (keys(%ENV)) {
        $envStr .= $k . "=" . $ENV{$k} . " " if ($k =~ /HADOOP/ || $k =~ /HIVE/);
    }
    $envStr .= " ";
    print $log "Going to run hive command [" . join(" ", @cmd) .
        "] with environment set to [$envStr]\n";
    my $runrc = run(\@cmd, \undef, $outfile, $errfile);
    my $rc = $? >> 8;

    return $runrc if $runrc; # success

    if (defined($noFailOnFail) && $noFailOnFail) {
        return $rc;
    } else {
        die "Failed running hive command [" . join(" ", @cmd) . "]\n";
    }
}

##############################################################################
#  Sub: runHadoopCmd
#
#  Run the provided hadoop command
# 
#  Returns:
#  Nothing
sub runHadoopCmd($$$)
{
    my ($cfg, $log, $c) = @_;

    my @cmd = ("$ENV{'HADOOP_HOME'}/bin/hadoop");
    push(@cmd, split(' ', $c));

    print $log "Going to run [" . join(" ", @cmd) . "]\n";

    run(\@cmd, \undef, $log, $log) or
        die "Failed running " . join(" ", @cmd) . "\n";
}

##############################################################################
#  Sub: runDbCmd
#
#  Run the provided mysql command
# 
#  Returns:
#  Nothing
sub runDbCmd($$$;$)
{
    my ($cfg, $log, $sqlfile, $outfile) = @_;

    $outfile = $log if (!defined($outfile));

    open(SQL, "< $sqlfile") or die "Unable to open $sqlfile for reading, $!\n";

    my @cmd = ('mysql', '-u', $cfg->{'dbuser'}, '-D', $cfg->{'dbdb'},
        '-h', $cfg->{'dbhost'}, "--password=$cfg->{'dbpasswd'}",
        "--skip-column-names");

    print $log "Going to run [" . join(" ", @cmd) . "] passing in [$sqlfile]\n";

    run(\@cmd, \*SQL, $outfile, $log) or
        die "Failed running " . join(" ", @cmd) . "\n";
    close(SQL);
}



1;
