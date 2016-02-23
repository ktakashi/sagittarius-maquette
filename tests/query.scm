(import (rnrs)
	(sagittarius)
	(maquette query)
	(dbi)
	(clos user)
	(srfi :117)
	(srfi :64)
	(text sql))

(include "class-defs.scm")

(test-begin "Maquette - Query")

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
(test-equal "SQL building (select person 2)"
	    '(select (id firstNames lastName addressId) (from person)
		     (where (= addressId ?)))
	    (maquette-build-select-statement 
	     <person> `(= address ,(make <address> :id 1))))
(test-equal "SQL building (select person 3)"
	    '(select (id firstNames lastName addressId) (from person)
		     (where (in addressId (? ?))))
	    (maquette-build-select-statement 
	     <person> `(in address 
			   ,(make <address> :id 1)
			   ,(make <address> :id 2))))
(test-equal "SQL building (select person 4)"
	    '(select (id firstNames lastName addressId) (from person)
		     (where (and (not (in addressId (? ?)))
				 (= firstNames ?))))
	    (maquette-build-select-statement 
	     <person> `(and (not (in address 
				     ,(make <address> :id 1)
				     ,(make <address> :id 2)))
			    (= first-names "Takashi"))))

(test-equal "SQL building (select person 5)"
	    '(select (id firstNames lastName addressId) (from person)
		     (where (= addressId (select (id) (from address)
						 (where (and (= city ?)))))))
	    (maquette-build-select-statement 
	     <person> `(= address ,(make <address> :city "Leiden"))))

(test-equal "SQL building (select collect)"
	    '(1 2)
	    (let ((queue (list-queue)))
	      (maquette-build-select-statement 
	       <person> `(in address 
			     ,(make <address> :id 1)
			     ,(make <address> :id 2))
	       queue)
	      (list-queue-list queue)))
(test-equal "SQL building (select collect 2)"
	    '(1 2 "Takashi")
	    (let ((queue (list-queue)))
	      (maquette-build-select-statement 
	       <person> `(and (not (in address 
				       ,(make <address> :id 1)
				       ,(make <address> :id 2)))
			      (= first-names "Takashi"))
	       queue)
	      (list-queue-list queue)))

(test-equal "SQL building (select collect 3)"
	    '("Leiden")
	    (let ((queue (list-queue)))
	      (maquette-build-select-statement 
	       <person> `(= address ,(make <address> :city "Leiden"))
	       queue)
	      (list-queue-list queue)))

(test-equal "SQL building (insert person)"
	    '(insert-into person (id firstNames lastName addressId)
			  (values (? ? ? ?)))
	    (maquette-build-insert-statement <person> '()))

(test-equal "SQL building (update person)"
	    '(update person (set! (= firstNames ?)))
	    (maquette-build-update-statement <person> '(firstNames) #f))
(test-equal "SQL building (update person 2)"
	    '(update person (set! (= firstNames ?)) 
		     (where (= addressId ?)))
	    (maquette-build-update-statement <person> '(firstNames) 
	      `(= address ,(make <address> :id 1))))

(test-equal "SQL building (delete person)"
	    '(delete-from person)
	    (maquette-build-delete-statement <person> #f))
(test-equal "SQL building (delete person 2)"
	    '(delete-from person (where (= addressId ?)))
	    (maquette-build-delete-statement <person> 
	      `(= address ,(make <address> :id 1))))

(cond-expand
 ((library (dbd sqlite3))
  ;; first drop all
  (define db-file "test.db")
  (when (file-exists? db-file) (delete-file db-file))

  (define conn (dbi-connect (format "dbi:sqlite3:database=~a" db-file)
			    :auto-commit #f))

  ;; prepare tables
  (dbi-execute-using-connection! 
   conn (ssql->sql (maquette-build-create-statement <address>)))
  (dbi-execute-using-connection! 
   conn (ssql->sql (maquette-build-create-statement <person>)))
  (dbi-execute-using-connection! 
   conn (ssql->sql '(create-table address_seq ((id int)))))
  ;; insert initial value
  (dbi-execute-using-connection! 
   conn (ssql->sql '(insert-into address_seq (id) (values (0)))))

  ;; inserts some data
  (let* ((a (make <address> :city "Leiden"))
	 (p (make <person> :id 1 :first-names "Takashi"
		  :last-name "Kato" :address a)))
    (test-equal "maquette-insert" 1 (maquette-insert conn p))
    (test-assert (slot-bound? a 'id))

    (test-equal "maquette-select 1"
		1
		(let ((r (maquette-select conn <person> '(= id 1))))
		  (test-assert "result" (not (null? r)))
		  (test-assert "person?" (is-a? (car r) <person>))
		  (test-assert "address?" (is-a? (slot-ref (car r) 'address)
						 <address>))
		  (length r)))
    (let* ((a2 (make <address> :city "Leiden"))
	   (p2 (make <person> :id 2 :first-names "Takashi Bla"
		     :last-name "Kato" :address a2)))
      (test-equal "maquette-insert (2)" 1 (maquette-insert conn p2))
      (test-assert "generated id" 
		   (not (eqv? (slot-ref a 'id) (slot-ref a2 'id))))))

  (let* ((a (make <address> :city "Den Haag"))
	 (p (make <person> :id 3 :first-names "Takashi Bla"
		  :last-name "Kato" :address a)))
    (test-equal "maquette-insert (3)" 1 (maquette-insert conn p)))
  
  (test-equal "maquette-select (all)" 3
	      (length (maquette-select conn <person>)))
  (test-equal "maquette-select (sub querying)" 2 
	      (length (maquette-select conn <person>
		       `(in address ,(make <address> :city "Leiden")))))

  (test-equal "maquette-update" 1
	      (maquette-update conn (make <person> :id 3 
					  :first-names "Takashi Yey")))
  (test-equal "Takashi Yey"
	      (let ((r (maquette-select conn <person> `(= id 3))))
		(slot-ref (car r) 'first-names)))

  (test-equal "maquette-delete (1)" 1
	      (maquette-delete conn (make <person> :id 3)))
  (test-equal "maquette-delete (2)" 2
	      (maquette-delete conn (make <person> :last-name "Kato")))

  (dbi-close conn)
  (when (file-exists? db-file) (guard (e (else #t))(delete-file db-file)))

  )
 (else 
  (display "SKIPPED!" (current-error-port)) (newline (current-error-port))
  (display "Actual database access tests require SQLite3 binding" 
	   (current-error-port))
  (newline (current-error-port))
  ))

(test-end)
