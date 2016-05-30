(import (rnrs)
	(sagittarius)
	(clos user)
	(maquette)
	(srfi :64)
	;; for creating table
	(maquette query)
	(maquette connection)
	(maquette context)
	(text sql)
	(dbi))

(include "class-defs.scm")

(test-begin "Maquette")

(define db-file "test.db")
(define dsn (format "dbi:sqlite3:database=~a" db-file))

;; basics
(test-assert (make-maquette-context dsn))
(test-assert (maquette-context? (make-maquette-context dsn)))
(test-equal 10 (maquette-max-connection-count (make-maquette-context dsn)))
(test-equal 5 (maquette-max-connection-count
		(make-maquette-context dsn :max-connection 5)))
(test-error  (make-maquette-context dsn :max-connection 0))

(test-equal 'ok
	    (let ((ctx (make-maquette-context dsn)))
	      (with-maquette-transaction ctx
	        (test-assert (maquette-context-in-transaction? ctx))
		'ok)))

(cond-expand
 ((library (dbd sqlite3))
  (when (file-exists? db-file) (delete-file db-file))

  (define ctx (make-maquette-context dsn :auto-commit #f :cache-type #f))
  ;; create tables for test
  (call-with-maquette-connection ctx
    (lambda (mc)
      (define raw-conn (maquette-connection-dbi-connection mc))
      (dbi-execute-using-connection! raw-conn 
       "CREATE TABLE address (id INTEGER PRIMARY KEY AUTOINCREMENT, city VARCHAR NOT NULL)")
      (dbi-execute-using-connection! raw-conn 
       "CREATE TABLE photo (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB)")
      (dbi-execute-using-connection! 
       raw-conn (ssql->sql (maquette-build-create-statement <person>)))
      
      (dbi-execute-using-connection! raw-conn
	"insert into address (id, city) values (0, 'Leiden')")
      (dbi-execute-using-connection! raw-conn
	"insert into sqlite_sequence (name, seq) values ('address', 0)")
      (dbi-execute-using-connection! raw-conn
	"insert into sqlite_sequence (name, seq) values ('photo', 0)")
      (maquette-connection-commit! mc)
      ))

  (let* ((a (make <address> :city "Leiden"))
	 (i (make <photo> :data #vu8(1 2 3 4 5)))
	 (p (make <person> :id 1 :first-names "Takashi"
		  :last-name "Kato" :address a :photo i)))
    (with-maquette-transaction ctx
      (test-assert (is-a? (maquette-save ctx a) <address>))
      (test-assert (is-a? (maquette-save ctx p) <person>))
      )

    (test-equal 1 (length (maquette-query ctx <address>
					  `(= id ,(slot-ref a 'id)))))
    (test-equal 1 (length (maquette-query ctx <person> '(= id 1)))))
  (with-maquette-transaction ctx
      (test-equal 1 (maquette-remove ctx (make <person> :id 1))))
  
  (call-with-maquette-connection ctx
    (lambda (conn)
      (for-each (lambda (p)
		  (test-equal "lazy" #vu8(1 2 3 4 5) 
			      (get-bytevector-all (slot-ref p 'data))))
		(maquette-query ctx <photo> #f))))

  (for-each (lambda (p)
	      (test-error "lazy (error)" error?
			  (get-bytevector-all (slot-ref p 'data))))
	    (maquette-query ctx <photo> #f))

  (test-assert (null? (maquette-query ctx <person> '(= id 1))))
  
  (test-assert (maquette-context-release! ctx))

  (when (file-exists? db-file) (delete-file db-file))
  )
 (else (display "(dbd sqlite3) is required for test" (current-error-port))
       (newline (current-error-port))))

(test-end)
