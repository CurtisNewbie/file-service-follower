# file-service-follower v1.0.0
 
Go service for backing up file-server's data. 

Maintaining an exact copy of the file-server is not the responsibility of this service. It assumes that the database is properly managed and backed-up. Only the files are saved.

So if the disk of the file-server is somehow broken, we still have the files. we can simply get a new disk, and copy all these files back to the disk of the server. Other than that, we may also need to update the `fs_group` and the `fs_group_id` in the table, but that should be very easy.

