# SQL translator

Originally created as a backend of a project.
API to handle Database connections and translate commands as comma separated values(CSV) 
 * coming from the client to MYSQL statements, used by the server to query the 
 * database, and return the resultset as an ArrayList of appropriate objects.
 *  
 *  Recognised commands:
 *   - LOGIN 
 *   - BOOK_TRIP
 *   - VIEW_TRIP
 *   - CANCEL_TRIP
 *   - ASSIGN_DRIVER
 *   - SIGN_UP
 *   - AVAILABLE_DRIVERS
 *   - GET_CARDS