(import (rnrs)
	(sagittarius)
	(maquette query)
	(dbi)
	(srfi :64)
	(text sql))

(cond-expand
 ((library (dbd sqlite3))

  (test-begin "Maquette - Query")

  (include "class-defs.scm")
  ;; first drop all
  (define db-file "test.db")
  (when (file-exists? db-file) (delete-file db-file))

  ;; these are only for my convenience
  (test-equal "SQL building (create addres)"
	      '(create-table address 
			     ((id int)
			      (city varchar (constraint not-null))
			      (constraint (primary-key id))))
	      (maquette-build-create-statement <address>))

  (test-equal "SQL building (create person)"
	      '(create-table person
			     ((id bigint)
			      (firstNames (varchar 255))
			      (lastName (varchar 50))
			      (addressId int)
			      (constraint (primary-key id))
			      (constraint 
			       (foreign-key (addressId)
					    (references address id)))))
	      (maquette-build-create-statement <person>))
  
  (test-equal "SQL building (select address)"
	      '(select (id city) (from address))
	      (maquette-build-select-statement <address> #f))
  (test-equal "SQL building (select person)"
	      '(select (id firstNames lastName addressId) (from person))
	      (maquette-build-select-statement <person> #f))

  (define conn (dbi-connect (format "dbi:sqlite3:database=~a" db-file)))

  ;; prepare tables
  (dbi-execute-using-connection! 
   conn (ssql->sql (maquette-build-create-statement <address>)))
  (dbi-execute-using-connection! 
   conn (ssql->sql (maquette-build-create-statement <person>)))
  
  ;; inserts some data
  
  (dbi-close conn)
  (when (file-exists? db-file) (delete-file db-file))
  (test-end)
  )
 (else 
  (display "SKIPPED!" (current-error-port))
  (display "Query test requires SQLite3 binding" (current-error-port))
  (newline (current-error-port))
  ))
