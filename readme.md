## Problem

we are currently at the stage of needed redis library thats smart enough to handle connections changes, after looking at library out there, non of statisfied our need, usual redis sentinel is always connect to master which is we wanted to balancing load from slave as well, so we need smart routing the commands what goes to master what goes to slave, we also need to make no down time when master is down, by sentinel default it would promote in the background new master, and our clients library will re-establish it topology reconnect without glitch in the client, so we need to reconnect, if master,slave, or sentinel dies, it known and smart enough to handle the changes

- we want to define new input it can be like this process.env.REDIS_URL = `redis+sentinel://host1:port,host2:port,host3:port` this is direct reference of sentinel node this is not redis master and slave, it can be multiple sentinel node
- we got the redis master and slave informations by going in sent command into those sentinel,`SENTINEL get-master-addr-by-name <name>; SENTINEL replicas <name>`
- we can get changes of those master and slave replications by listening to various events, such us `+switch-master, +slave, +sdown, -sdown, +odown, +failover-end`
- after we got the who is master and who is slave, we gonna use master for writing command and slave for reading commands, first we establish 2 connections, connect two seperate instance for read and for write
- read can be balancing randomly from slave, 
- we will have command mapping or routing, if we got READ type of command we sent through slave, and WRITE type always goes to master, any unrecognize command we sent through master
- we need to setup in the setup to replica-announce-ip to use public ip so it reachable from clients because it assume client connect from node.js
- we wanted seamless experiences no glitchy, even when network hiccup
- we need to establish a couple of scenario and specifications that i will describe bellow, 

