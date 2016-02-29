(import (rnrs)
	(sagittarius)
	(maquette connection)
	(dbi)
	(clos user)
	(srfi :117)
	(srfi :64)
	(text sql))

(test-begin "Maquette - Connection")

(define db-file "test.db")
(define dsn (format "dbi:sqlite3:database=~a" db-file))

(test-assert (make-maquette-connection dsn))
(test-assert (maquette-connection?
	      (make-maquette-connection dsn)))

(test-assert (make-maquette-connection-pool 1 dsn))
(test-assert (maquette-connection-pool?
	      (make-maquette-connection-pool 1 dsn)))

(test-error assertion-violation?
	    (maquette-connection-pool-return-connection
	      (make-maquette-connection-pool 1 dsn)
	      (make-maquette-connection dsn)))

(test-equal 10 (maquette-connection-pool-max-connection-count
		(make-maquette-connection-pool 10 dsn)))

(cond-expand
 ((library (dbd sqlite3))
  ;; first drop all
  (when (file-exists? db-file) (delete-file db-file))

  ;; auto-commit #t would be better for testing
  (define connection (open-maquette-connection dsn :auto-commit #t))
  ;; for testing 1 is enough
  (define pool (make-maquette-connection-pool 1 dsn :auto-commit #t))

  ;; connection
  (test-assert (maquette-connection-open? connection))
  (test-assert (maquette-connection? 
		(maquette-connection-connect! connection)))
  (let ((old (maquette-connection-dbi-connection connection)))
    (test-assert
     (maquette-connection? (maquette-connection-connect! connection)))
    ;; maquette-connection-connect! doesn't do anything if connection
    ;; is not closed.
    (test-equal old
		(maquette-connection-dbi-connection
		 (maquette-connection-connect! connection)))
    (test-assert (maquette-connection? 
		  (maquette-connection-close! connection)))
    (test-assert "double closing"
		 (maquette-connection? 
		  (maquette-connection-close! connection)))
    (test-assert (not (maquette-connection-open? connection)))
    (test-assert (maquette-connection? 
		  (maquette-connection-reconnect! connection))))
  (let ((old (maquette-connection-dbi-connection connection)))
    ;; reconnection actually re-connect the connection
    ;; so not be the same
    (test-assert 
     (not (eq? old
	       (maquette-connection-dbi-connection 
		(maquette-connection-reconnect! connection))))))

  (test-assert (maquette-connection-start-transaction! connection))
  (test-assert (maquette-connection-commit! connection))
  (test-assert (maquette-connection-start-transaction! connection))
  (test-assert (maquette-connection-rollback! connection))

  (test-equal 'ok (with-maquette-connection-transaction connection 'ok))

  ;; connection pool
  (let ((c (maquette-connection-pool-get-connection pool)))
    (test-assert (maquette-connection? c))
    (test-assert (maquette-connection-open? c))
    (test-assert (not (maquette-connection-pool-connection-available? pool)))

    (test-equal 'timeout
		(maquette-connection-pool-get-connection pool 0 'timeout))

    (maquette-connection-close! c)
    (maquette-connection-pool-return-connection pool c)
    (let ((c2 (maquette-connection-pool-get-connection pool)))
      (test-assert (maquette-connection-open? c))
      (maquette-connection-pool-return-connection pool c))
    ;; oops
    (test-equal 1
		(maquette-connection-pool-available-connection-count
		 (maquette-connection-pool-return-connection pool c)))

    (test-assert (maquette-connection-pool?
		  (maquette-connection-pool-release! pool)))
    (test-assert (not (maquette-connection-open? c)))


    ;; re-opened
    (test-assert (maquette-connection-pool-re-pool! pool))
    (test-assert (maquette-connection-pool-connection-available? pool))
    ;; no longer in the same pool
    (test-error assertion-violation?
		(maquette-connection-pool-return-connection pool c))

    (test-equal 'ok (call-with-available-connection pool (lambda (conn) 'ok)))

    )
  
  (when (file-exists? db-file) (guard (e (else #t))(delete-file db-file)))

  )
 (else 
  (display "SKIPPED!" (current-error-port)) (newline (current-error-port))
  (display "Actual database access tests require SQLite3 binding" 
	   (current-error-port))
  (newline (current-error-port))
  ))

(test-end)
