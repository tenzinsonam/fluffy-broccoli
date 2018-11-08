# fluffy-broccoli

create table named filmorder with columns film_id,title, rental_rate

#TODO
1. Add support for command, 
		updatetill <time in minutes>
	The post will be valid till that time, 

	Add another column "expires" in the database table status
	This stores the time till when the entry would expire
	Fetch the requested entry, if it is already expired (NOW()>expires)
	then delete those entries and return empty corresponding to those entries

2. Add support for deletei <my_postno>
	Also for delete_range <starting postno> <ending postno>
	We would be using Smart Delete Strategy, according to which
	dhruvkmr#0 -> Post_no of the last post (as before)
	dhruvkmr#-1 -> The post_nos of deleted posts numbered from 1-100
	dhruvkmr#-2 -> Post_nos of deleted posts numbered from 101-200

	and so on

	If I need to print last 233 posts for user with dhruvkmr#0=455
	I'll check dhruvkmr#-5 to get deleted postnos from 401-500
	Then I'll check dhruvkmr#-4 to get deleted postnos from 301-400
	I'll also check dhruvkmr#-3 to get deletd postnos from 201-300
	And if the no. of remaining posts from 201-455 is <233
	Then I'll also need to check delete posts from 101-200, using the 
	value of dhruvkmr#-2

	Note, separate the values of dhruvkmr#-1 (etc)  by <space> or comma

	Upon deletion of a post, if it's postno == dhruvkmr#0's value. Then
	decrement it, making use of dhruvkmr#-4 or (-5 etc, whatever), to set
	the value of dhruvkmr#0 to the postno of last undeleted post with maximum
	post value

3. Add support for '#latest<timestamp>' key whose value would be the username#postid 
	of the newly added post at time <timestamp>
	Set the expiration time of this key in memcached to be 20 seconds
	Clearly if a new post comes at same timestamp, append its' key to the value of #latest<timestamp>

	CLearly there would be atmost 20 #latest<timestamp> entries in the memcache

	If I need to get the latest posts. Get the current timestamp = currtmp
	search for #latest<currtmp>, #latest<currtmp-1> ... #latest<currtmp-20>

	DO NOT STORE #latest<timestamp> in database.
	Neither do search for this key in database, if memcahed search fails
	It's just a temporary storage for latest update, utilizing memcached expiration policy


