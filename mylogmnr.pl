#!/usr/bin/perl
use strict;
use POSIX ':signal_h';
use Getopt::Long;    
Getopt::Long::Configure qw(no_ignore_case);
use POSIX qw(strftime); 
use warnings;


my %opt;

my $port = 3306;                              
my $user = "qry";
my $pswd;
my $lhost;
my $sqlf;
my $tbn;
my $op="redo";

my $state="init";
my $col=0;
my $tabname;

my $sql;
my $undosql;
my $var;
my $varcol;
my $type;

my $updold=1;
my $wheresql;
my $setsql; 

my $undo_wheresql;
my $undo_setsql; 

my %tabhash;
  
&scanlogmain();




# ----------------------------------------------------------------------------------------
# Func :  scanlogmain
# ----------------------------------------------------------------------------------------
sub scanlogmain(){
    my $newfilename;
    
	# Get options info
	&get_options();
	
	if("undo" eq $op){
		$newfilename=$sqlf . ".undo_tmp";
	}else{
		$newfilename=$sqlf . ".redo";
	}
	
	
    open LOGMNR_REPORT ,">:encoding(utf-8)", "$newfilename" or die ("Can't open $newfilename for write! /n");

	
	open SQLFILE ,"<:encoding(utf-8)","$sqlf" or  die ("Can't open config file: $sqlf\n\n");
    while(<SQLFILE> ){
    	chomp;
    	#print $_ . "\n";
		if ($_ =~ m/^BEGIN/) {
			$sql='BEGIN;';
			$undosql='COMMIT;';
			if($op eq "undo"){
				print  LOGMNR_REPORT "$undosql \n";
			}else{
				print  LOGMNR_REPORT "$sql \n";
			}			
			
		}elsif($_ =~ m/^COMMIT/){
			$sql='COMMIT;';
			$undosql='BEGIN;';
			if($op eq "undo"){
				print  LOGMNR_REPORT "$undosql \n";
			}else{
				print  LOGMNR_REPORT "$sql \n";
			}	
		}elsif($_ =~ m/^ROLLBACK/){
			$sql='ROLLBACK;';
			print LOGMNR_REPORT "$sql \n";
		}elsif(index($_,"Table_map:")>-1){
			$sql=$_;
			print LOGMNR_REPORT "$sql \n";
		}
		
		
		if ($state eq "init") {
			if($_ =~ m/^### UPDATE ([a-zA-Z.`_0-9]+)/) {
				 $_=$1;
				 s/`//g;
				 $tabname=$_;
				 $state="upd";
				 $type = "UPDATE";
				&get_tabcolumns($tabname);
				 
			}elsif ($_ =~ m/^### INSERT INTO ([a-zA-Z.`_0-9]+)/) {
				 $_=$1;
				 s/`//g;
				 $tabname=$_;
				 $state="ins";
				 $type = "INSERT";
				 &get_tabcolumns($tabname);
			} elsif ($_ =~ m/^### DELETE FROM ([a-zA-Z.`_0-9]+)/) {
				 $_=$1;
				 s/`//g;
				 $tabname=$_;
				 $state="del";
				 $type = "DELETE";
				 &get_tabcolumns($tabname);
			}
		}
			
	
		if ($state eq "upd") {
			if ($_ =~ m/^### WHERE/) {           
				$state = "coll";
				$updold=1;
			}
			$col = 0;
		}
		if ($state eq "ins") {
			if ($_ =~ m/^### SET/) {           
				$state = "coll";
			}
			$col = 0;
		}
		if ($state eq "del") {
			if ($_ =~ m/^### WHERE/) {           
				$state = "coll";
			}
			$col = 0;
		}
		if ($state eq "coll") {
			if ($type eq "UPDATE") {
				if ($_ =~ m/^### SET/) {
					$setsql=" SET ";
					$undo_wheresql=" WHERE " ;          
					$updold=0;
				}
			}
			
		   if ($_ =~ m/^###   @([0-9]+)=(.*)/s ) {
			   #print "$1, $2\n";
			   # 2012-04-21 12:43:22
			   $varcol=$1;
			   $var = $2;
			   if ($var =~ m/([0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9])/) {
				   $var = "'$1'";
			   }
			   if ($var =~ m/(-[0-9]+) \([0-9]+\)/) {
				   $var = $1;
			   }
			   if ($var =~ m/^'(.*)'$/s) {
			   	   $_=$1;
				   s/'/\\'/g;
				   $var = "'" . $_ . "'";
			   }			   
			   if ($col == 0) {
			   		if($type eq "UPDATE" && $updold==1){
			   			$sql = "UPDATE $tabname";
			   			$wheresql=" WHERE ${$tabhash{$tabname}}[$varcol]" . &format_nullstr($var);
			   			$undosql="UPDATE $tabname SET ";
			   			$undo_setsql="${$tabhash{$tabname}}[$varcol]=$var";
			   		}elsif($type eq "DELETE"){
			   			$sql = "DELETE FROM $tabname where ${$tabhash{$tabname}}[$varcol]" . &format_nullstr($var);
			   			$undosql = "INSERT INTO $tabname  VALUES( $var ";
			   		}elsif($type eq "INSERT"){
			   			$sql = "INSERT INTO $tabname  VALUES( $var ";
			   			$undosql = "DELETE FROM $tabname where ${$tabhash{$tabname}}[$varcol]" . &format_nullstr($var);
			   		}
				  
			   } else { 
			   		if($type eq "UPDATE" && $updold==1){
			   			$wheresql=$wheresql . " and ${$tabhash{$tabname}}[$varcol]" . &format_nullstr($var);
			   			$undo_setsql=$undo_setsql . " ,${$tabhash{$tabname}}[$varcol]=$var";
			   		}elsif($type eq "UPDATE" && $updold==0){		   				
		   				if($setsql eq " SET "){
		   					$setsql=$setsql . "${$tabhash{$tabname}}[$varcol]=$var";
		   				}else{
		   					$setsql=$setsql . ",${$tabhash{$tabname}}[$varcol]=$var";
		   				}
		   				if($undo_wheresql eq " WHERE "){
		   					$undo_wheresql=$undo_wheresql . " ${$tabhash{$tabname}}[$varcol]" . &format_nullstr($var);
		   				}else{
		   					$undo_wheresql=$undo_wheresql . " and ${$tabhash{$tabname}}[$varcol]" . &format_nullstr($var);
		   				}
		   				
			   		}elsif($type eq "DELETE"){
			   			$sql = $sql . " and ${$tabhash{$tabname}}[$varcol]" . &format_nullstr($var);
			   			$undosql = $undosql . ", $var";
			   		}elsif($type eq "INSERT"){
			   			$sql = $sql . ", $var";
			   			$undosql = $undosql . " and ${$tabhash{$tabname}}[$varcol]" . &format_nullstr($var);
			   		}
				  
			   }
			   $col++;
		   } elsif ($_ =~ m/^# at [0-9]+$/) {
			   $state="init";
			   		if($type eq "UPDATE"){
			   			$sql = $sql . $setsql . $wheresql .";";
			   			$undosql = $undosql . $undo_setsql . $undo_wheresql .";";
			   		}elsif($type eq "DELETE"){
			   			$sql = $sql . ";";
			   			$undosql = $undosql .  ");";
			   		}elsif($type eq "INSERT"){
			   			$sql = $sql .  ");";
			   			$undosql = $undosql . ";";
			   		}
			   		if(defined $tbn &&  $tbn ne "" && $tabname eq $tbn ){
						if($op eq "undo"){
							print  LOGMNR_REPORT "$undosql \n";
						}else{
							print  LOGMNR_REPORT "$sql \n";
						}	
			   		}elsif(!(defined $tbn) || $tbn eq ""){
						if($op eq "undo"){
							print  LOGMNR_REPORT "$undosql \n";
						}else{
							print  LOGMNR_REPORT "$sql \n";
						}	
			   		}			   
			   
		   } elsif ($_ =~ m/^### UPDATE ([a-zA-Z.`_0-9]+)/) {
			   $sql = $sql . $setsql . $wheresql .";";
			   $undosql = $undosql . $undo_setsql . $undo_wheresql .";";
		   		if(defined $tbn &&  $tbn ne "" && $tabname eq $tbn ){
					if($op eq "undo"){
						print  LOGMNR_REPORT "$undosql \n";
					}else{
						print  LOGMNR_REPORT "$sql \n";
					}		   			
		   		}elsif(!(defined $tbn) || $tbn eq ""){
					if($op eq "undo"){
						print  LOGMNR_REPORT "$undosql \n";
					}else{
						print  LOGMNR_REPORT "$sql \n";
					}	
		   		}
			   $_=$1;
			   s/`//g;
			   $tabname=$_;
			   $state="upd";
			   &get_tabcolumns($tabname);
		   } elsif ($_ =~ m/^### INSERT INTO ([a-zA-Z.`_0-9]+)/) {
			   $sql = $sql . ");";
			   $undosql = $undosql . ";";
		   		if(defined $tbn &&  $tbn ne "" && $tabname eq $tbn ){
					if($op eq "undo"){
						print  LOGMNR_REPORT "$undosql \n";
					}else{
						print  LOGMNR_REPORT "$sql \n";
					}	
		   		}elsif(!(defined $tbn) || $tbn eq ""){
					if($op eq "undo"){
						print  LOGMNR_REPORT "$undosql \n";
					}else{
						print  LOGMNR_REPORT "$sql \n";
					}	
		   		}
			   $_=$1;
			   s/`//g;
			   $tabname=$_;
			   $state="ins";
			   &get_tabcolumns($tabname);
		   } elsif ($_ =~ m/^### DELETE FROM ([a-zA-Z.`_0-9]+)/) {
			   $sql = $sql . ";";
			   $undosql = $undosql .  ");";
		   		if(defined $tbn &&  $tbn ne "" && $tabname eq $tbn ){
					if($op eq "undo"){
						print  LOGMNR_REPORT "$undosql \n";
					}else{
						print  LOGMNR_REPORT "$sql \n";
					}	
		   		}elsif(!(defined $tbn) || $tbn eq ""){
					if($op eq "undo"){
						print  LOGMNR_REPORT "$undosql \n";
					}else{
						print  LOGMNR_REPORT "$sql \n";
					}	
		   		}
			   $_=$1;
			   s/`//g;
			   $tabname=$_;
			   $state="del";
			  &get_tabcolumns($tabname);
		   }
		
			
		}	
     
	}
	  
	close SQLFILE or die "Can't close file(SQLFILE)!";
	close LOGMNR_REPORT or die "Can't close file(LOGMNR_REPORT)!";
	
	if("undo" eq $op){
	    `perl -e 'print reverse <>' $newfilename >  "$sqlf.undo"`;
	}	
	
}

# ----------------------------------------------------------------------------------------
# 
# Func : get table columns
# ----------------------------------------------------------------------------------------
sub get_tabcolumns {
    use DBI;    
	use DBI qw(:sql_types);
	my $dbtable=shift;	
	my @result = split( /\./, $dbtable );	
	my @columns;
		
	unless($tabhash{$dbtable}){
		my $dbh = DBI->connect( "DBI:mysql:database=information_schema;host=$lhost;port=$port","$user", "$pswd", { 'RaiseError' => 0 ,AutoCommit => 1} );
		if(not $dbh) {
			return;
		}
	    
		my	$sth = $dbh->prepare("select COLUMN_NAME ,ORDINAL_POSITION from COLUMNS where table_name=\'$result[1]\' and TABLE_SCHEMA=\'$result[0]\' order by  ORDINAL_POSITION asc");
		$sth->execute();
		while( my @result2 = $sth->fetchrow_array )	{
			$columns[$result2[1]]=$result2[0];
		}
		
	    $sth->finish;
		$dbh->disconnect();
		$tabhash{"$dbtable"}=\@columns;

	}
}


# ----------------------------------------------------------------------------------------
# Func :  print usage
# ----------------------------------------------------------------------------------------
sub print_usage {

	#print BLUE(),BOLD(),<<EOF,RESET();
	print <<EOF;

==========================================================================================
Info  :
        Created By noodba (www.noodba.com) .
      Modified from parse_binlog.pl by  junda\@alipay.com
        Just use it for testing or studying
Usage :
Command line options :

   -h,--help           Print Help Info. 
   -P,--port           Port number to use for local mysql connection(default 3306).
   -u,--user           user name for local mysql(default qry).
   -p,--pswd           user password for local mysql(can't be null).
   -lh,--lhost         ip for mysql where info is got(can't be null).  
   -f,--sqlf           the sql file which will be parsed.
   -o,--op             redo sql or undo sql(default redo sql)
   -t,--tbn            table name   
Sample : 
   shell> perl mylogmnr.pl -u qry -p 123456 -f /tmp/aaa.sql
==========================================================================================
EOF
	exit;
}

# ----------------------------------------------------------------------------------------
# Func : get options and set option flag
# ----------------------------------------------------------------------------------------
sub get_options {

	# Get options info
	GetOptions(
		\%opt,
		'h|help',          # OUT : print help info
		'P|port=i',        # IN  : port
		'u|user=s',        # IN  : user
		'p|pswd=s',        # IN  : password
		'lh|lhost=s',      # IN  : host		
		'f|sqlf=s',      # IN  : sqlf
		'o|op=s',      # IN  : operation
		't|tbn=s',      # IN  : table name
		'l|level=s'
	) or print_usage();

	if ( !scalar(%opt) ) {
		&print_usage();
	}

	# Handle for options
	$opt{'h'}  and print_usage();
	$opt{'P'}  and $port = $opt{'P'};
	$opt{'u'}  and $user = $opt{'u'};
	$opt{'p'}  and $pswd = $opt{'p'};
	$opt{'lh'} and $lhost = $opt{'lh'};
	$opt{'f'} and $sqlf = $opt{'f'};
	$opt{'o'} and $op= $opt{'o'};
	$opt{'t'} and $tbn = $opt{'t'};


	if (
		!(
			     defined $pswd
			and defined $lhost
			and defined $sqlf
		)
	  )
	{  
		&print_usage();
	}
}


sub format_nullstr() {
	my $str = shift;
	
    if($str eq 'NULL'){
    	return " IS NULL";
    }else{
    	return "=$str";
    }	
}
