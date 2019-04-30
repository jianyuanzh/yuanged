#!/usr/bin/expect

# Usage: ./login.sh my_pass root 10.130.161.1
set timeout 30
spawn ssh -p [lindex ${argv} 0] [lindex ${argv} 1]@[lindex $argv 2]
expect {
        "(yes/no)?"
        {send "yes\n";exp_continue}
        "password:"
        {send "[lindex $argv 3]\n"}
}
interact
