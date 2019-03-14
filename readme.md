
0. Java version 9 or upper level.
1. Build the project.
2. Under /out/production/Calendar/, open 3 terminal windows, run
	java Main wuu <nodeID(a integer, 0, 1, or 2)>
start nodes using Wuu-Bernstein Algorithm, or
	java Main paxos <nodeID>
start node using Paxos Algorithm. 
4. The format of add 
	add <appointmentName> <day> <startTime> <endTime> <participants>
5. The format of delete
	delete <appointmentID>
6. The format of view
	view
7. The format of view all
	view all
