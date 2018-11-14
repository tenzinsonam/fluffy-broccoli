
Procedure

1. Run server using command, 'python3 server.py <portno>'
2. Run client using command, 'python3 client.py <same_portno_as_in_above_command>
3. If you are a new user, press 'Y', then enter.
4. If you are already existing user, type your username
5. After you are logged in you can use any of the following commands 
    
    5.1 search <username> <#ofPosts>
    5.2 update <enter>
           <Your Post>
    5.3 deletme
    5.4 getlatest
    5.5 deletei <postno>
    5.6 deletei <starting_post_num> <ending_post_no>
    5.7 exit

6. The implications of above commands are given below

•Search: This allows the user to search for any registered username and also allows him to show the last n (specified as an argument) commentsmade by him.  But for this to function, it is necessary to priorly knowthe username of the person to be searched for.

•Update:  This enable the user to write a new comment which can beassociated with him.  This comment posted by this query will not haveany expiry time and thus will be present permanetly in the databaseuntill deleted explicitly by the user by calling some other query

•Deleteme: This query allows the user to delete its entire footprint fromthe  database  along  with  any  data  relevant  to  him  in  the  memcache.Some  restraint  is  required  while  using  this  command  as  there  is  nogoing back after the deed is done.  (as there is no secondary alerts afterissuing the command to warn the user)

•DeleteI:  This  enables  the  user  to  delte  any  comment  which  he/shemay feel are not relevant to him/her anymore and provides a wide arryof functionality.  It allows the user to handpick comments one by oneor the comments which lie in a particular range can be deleted.  Againthere  is  no  coming  back  once  the  command  has  been  issued  as  thecomments will be deleted both from the cache and the database

.•getLatest:  This query relies heavily upon the memcache and the timeto  live  functionality  provide  by  it.   It  essentially  allows  the  user  toshow all the posts which have been posted to the server in the last 20seconds.  It relies on the fact that while updating a query the TTL forits availability in the cache can be specified.  This essentially allows usto implement this function without any extra costs and worring aboutthe cache getting full of values


