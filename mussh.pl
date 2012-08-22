#!/usr/bin/env perl
use warnings;
use strict;
use version;

use File::Basename;
use Net::OpenSSH;
use Parallel::ForkManager;
use Data::Dumper;
use Config::Simple;
use Getopt::Long;
use File::Basename qw(basename dirname);
use FindBin qw($Bin);

# VERSION
our $VERSION = "v0.1";

# global cfg hash
our %cfg;

# common var
my $self_dir = $Bin;
my $self_name = basename(basename($0), ".pl");
my $self_conf = "$self_dir/$self_name".".conf";

# read config file
my $main_cfg = new Config::Simple($self_conf) || die Config::Simple->error();
my $cfg_self_opt = $main_cfg->param(-block => 'MAIN');
my $cfg_ssh_opt = $main_cfg->param(-block => 'SSH');
my $cfg_rsync_opt = $main_cfg->param(-block => 'RSYNC');
my $cfg_cmd_opt = $main_cfg->param(-block => 'COMMAND');

my ($cli_self_opt, $cli_ssh_opt, $cli_rsync_opt, $cli_cmd_opt);
my ($help, $debug) = (0, 0);
# parse cli options
GetOptions(
    'help|h'                => \$help,
    'debug=i'               => \$debug,
    'max-proc=i'            => \$cli_self_opt->{max_proc},

    'connect-timeout=i'     => \$cli_ssh_opt->{timeout},
    'command-timeout=i'     => \$cli_cmd_opt->{timeout},
    'sync-timeout=i'        => \$cli_rsync_opt->{timeout},
    'bwlimit=i'             => \$cli_rsync_opt->{bwlimit},

    'method|m=s'	    => \$cli_self_opt->{method},
    'host|h=s'              => \$cli_self_opt->{host},
    'user|u=s'              => \$cli_ssh_opt->{user},
    'password|p=s'	    => \$cli_ssh_opt->{password},
    'key-path|i:s'          => \$cli_ssh_opt->{key_path},

    'remote-cmd|c=s'        => \$cli_self_opt->{remote_cmd},
    'push-files|s=s{,}'     => \@{$cli_self_opt->{push_files}},
    'pull-files|g=s{,}'     => \@{$cli_self_opt->{pull_files}},
    'local-dir|l=s'         => \$cli_self_opt->{local_dir},
    'remote-dir|r=s'        => \$cli_self_opt->{remote_dir},
) or pod2usage(2);
pod2usage(1) if $help;

# scrub empty params in hash ref
#&scrub_empty_hash_ref($cli_self_opt, $cli_ssh_opt, $cli_rsync_opt, $cli_cmd_opt);
defined $cli_self_opt->{$_} or delete $cli_self_opt->{$_} for keys %{$cli_self_opt};
defined $cli_ssh_opt->{$_} or delete $cli_ssh_opt->{$_} for keys %{$cli_ssh_opt};
defined $cli_rsync_opt->{$_} or delete $cli_rsync_opt->{$_} for keys %{$cli_rsync_opt};
defined $cli_cmd_opt->{$_} or delete $cli_cmd_opt->{$_} for keys %{$cli_cmd_opt};

# combined parameters by priority as default -> config file -> command lines 
%cfg = (
    'ssh_opt' => {
	port		=> 36000,
	ssh_cmd		=> "/usr/bin/ssh",
	rsync_cmd	=> "/usr/bin/rsync",
	timeout		=> 10,
	strict_mode	=> 0,
	batch_mode	=> 1,
	kill_ssh_on_timeout => 1,
	user		=> $ENV{USER},
	key_path	=> "$ENV{HOME}/.ssh/id_rsa",
	master_opts	=> [ -o => "StrictHostKeyChecking=no" ],
	%{$cfg_ssh_opt},
	%{$cli_ssh_opt},
    },
    'self_opt' => {
	method		=> "password",
	max_proc	=> 200,
	%{$cfg_self_opt},
	%{$cli_self_opt},
    },
    'rsync_opt' => {
	timeout		=> 600,
	bwlimit		=> 102400,
	archive		=> 1,
	compress	=> 1,
	%{$cfg_rsync_opt},
	%{$cli_rsync_opt},
    },
    'command_opt' => {
	timeout		=> 600,
	%{$cfg_cmd_opt},
	%{$cli_cmd_opt},
    },
);

# load account info to %cfg
my %account;
my $acct = $main_cfg->param(-block => 'ACCOUNT');
for my $a (keys %{$acct}) {
    ($account{$a}->{user}, $account{$a}->{password}) = split(/:/, $acct->{$a}, 2);
}
$cfg{account} = \%account;
my $user = $cfg{ssh_opt}->{user};

# decide auth method
if($cfg{self_opt}->{method} eq "key") {
    delete $cfg{ssh_opt}->{password};
    die "no key file specified.\n" unless $cfg{ssh_opt}->{key_path};
} elsif($cfg{self_opt}->{method} eq "password") {
    delete $cfg{ssh_opt}->{key_path};
    $cfg{ssh_opt}->{password} = $cfg{account}->{$user}->{password} unless $cfg{ssh_opt}->{password};
} else {
    pod2usage(2);
}

print Dumper(\%cfg);
#exit 0;
my $hosts = [ qw(172.17.151.165 10.161.130.145) ];
parallel_job(\&ssh_job, $cfg{self_opt}, $hosts, 20);
# the most important function
# call ssh or rsync on remote side
# its behavior depends on \%opt
sub ssh_job {
    my ($ip, $opt, $return) = @_;

    # redirect master stderr to /dev/null unless debugging
    my $ssh_file = $debug >= 1 ? "log/master.$ip.log" : "/dev/null";
    open my $fh, "> $ssh_file" or die "$!";
    $cfg{ssh_opt}->{master_stderr_fh} = $fh,

    my $ssh = Net::OpenSSH->new($ip, %{$cfg{ssh_opt}});
    if ( $ssh->error ) {
        $return->{ssherr} = "ssh connect failed: " . $ssh->error;
        return 3;
    }

    # push files to remote dir
    if ( @{$opt->{push_files}} > 0) {
        $ssh->rsync_put( $cfg{rsync_opt}, @{$opt->{push_files}}, $opt->{remote_dir});
        if ( $ssh->error ) {
            $return->{ssherr} = "rsync push failed: " . $ssh->error;
            return 3;
        }
    }
    
    # run command on remote side
    if ( $opt->{remote_cmd} ) {
        ($return->{stdout}, $return->{stderr}) = $ssh->capture2($cfg{command_opt}, "$opt->{remote_cmd}");
        if ( $ssh->error ) {
            $return->{ssherr} = "operation didn't complete successfully: " . $ssh->error;
        }
    }

    # pull files to local
    if ( @{$opt->{pull_files}} > 0 ) {
        mkdir "$opt->{local_dir}/$ip" unless -d "$opt->{local_dir}/$ip";

        $ssh->rsync_get( $cfg{rsync_opt}, @{$opt->{pull_files}}, "$opt->{local_dir}/$ip");
        if ( $ssh->error ) {
            $return->{ssherr} = "rsync pull failed: " . $ssh->error;
            return 3;
        }
    }
}

# parallel any subroutine, args are:
# 1. function reference
# 2. hash reference of option of the functin specified by 1st arg
# 3. hosts array
# 4. maximam parallel process allowd
sub parallel_job {
    my ($sub, $opt, $array, $max_proc) = @_;
    my $pm = new Parallel::ForkManager($max_proc);
    my @results;

    $pm->run_on_finish ( # called BEFORE the first call to start()
	sub {
	    my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $return) = @_;

	    if (defined($return)) {  # children are not forced to send anything
		$return->{ip} = $ident;

		# ssherr conisder as error
		if($return->{ssherr}) {
		    $return->{exit_code}=1;
		} else {
		    $return->{exit_code}=0;
		}
		push @results, $return;
	    } else {  # problems occuring during storage or retrieval will throw a warning
		print qq|No message received from child process $pid!\n|;
	    }
	}
    );
    for my $h ( @{$array} ) {
	$pm->start($h) and next;	# do the fork
	my %return;
	$sub->($h, $opt, \%return);
	$pm->finish(0, \%return);
    }
    $pm->wait_all_children;
    process_results(\@results);
}

# display results in a custom way
sub process_results {
    my $results = shift;
    my $rt=1;

    my $date = `date "+%Y-%m-%d %H:%M"`;
    chomp($date);
    print "=" x 20, $date, "=" x 20, "\n";
    for my $r (@{$results}) {
	$rt = 0 if ($r->{exit_code} == 1);	# ssh return 0 for true, perl return non-zero for true
	printf "%-7s%-16s\n", ($r->{exit_code} ? "[FAIL]" : "[OK]"), "$r->{ip}:";
	printf "%-16s%-s", " ", $r->{stdout} if $r->{stdout};
    }
    print "\n", "=" x 56, "\n";

    # Summary
    my @fail = grep { $_->{exit_code} } @{$results};
    my @succ = grep { ! $_->{exit_code} } @{$results};
    print "\nSummary: [OK]: " . scalar @succ . " [FAIL]: " . scalar @fail . "\n";
    if(@fail) {
	print "Failed hosts: \n";
	print $_->{ip} . "\n" for @fail;
	print "\n";
    }
    return $rt;
}

