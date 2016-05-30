(import (rnrs)
	(sagittarius)
	(maquette query)
	(maquette connection)
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
			    (photoId int)
			    (constraint (primary-key id))
			    (constraint 
			     (foreign-key (addressId)
					  (references address id)))
			    (constraint 
			     (foreign-key (photoId)
					  (references photo id)))))
	    (maquette-build-create-statement <person>))

(test-equal "SQL building (select address)"
	    '(select (id city) (from address))
	    (maquette-build-select-statement <address> #f))
(test-equal "SQL building (select person)"
	    '(select (id firstNames lastName addressId photoId) (from person))
	    (maquette-build-select-statement <person> #f))
(test-equal "SQL building (select person 2)"
	    '(select (id firstNames lastName addressId photoId) (from person)
		     (where (= addressId ?)))
	    (maquette-build-select-statement 
	     <person> `(= address ,(make <address> :id 1))))
(test-equal "SQL building (select person 3)"
	    '(select (id firstNames lastName addressId photoId) (from person)
		     (where (in addressId (? ?))))
	    (maquette-build-select-statement 
	     <person> `(in address 
			   ,(make <address> :id 1)
			   ,(make <address> :id 2))))
(test-equal "SQL building (select person 4)"
	    '(select (id firstNames lastName addressId photoId) (from person)
		     (where (and (not (in addressId (? ?)))
				 (= firstNames ?))))
	    (maquette-build-select-statement 
	     <person> `(and (not (in address 
				     ,(make <address> :id 1)
				     ,(make <address> :id 2)))
			    (= first-names "Takashi"))))

(test-equal "SQL building (select person 5)"
	    '(select (id firstNames lastName addressId photoId) (from person)
		     (where (and (<= firstNames ?)
				 (>= firstNames ?)
				 (and (<= firstNames ?) (<= ? firstNames)))))
	    (maquette-build-select-statement 
	     <person> `(and (<= first-names "Takashi")
			    (>= first-names "Takashi")
			    (between first-names "Takashi" "Takashi"))))

(test-equal "SQL building (select person 5)"
	    '(select (id firstNames lastName addressId photoId) (from person)
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
	    '(insert-into person (id firstNames lastName addressId photoId)
			  (values (? ? ? ? ?)))
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

  (define conn (open-maquette-connection
		(format "dbi:sqlite3:database=~a" db-file)
		:auto-commit #t))
  (define raw-conn (maquette-connection-dbi-connection conn))

  ;; prepare tables
  ;; sucks!
  (dbi-execute-using-connection! 
   raw-conn "CREATE TABLE address (id INTEGER PRIMARY KEY AUTOINCREMENT, city VARCHAR NOT NULL)")
  (dbi-execute-using-connection! 
   raw-conn "CREATE TABLE photo (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB)")
  (dbi-execute-using-connection! 
   raw-conn (ssql->sql (maquette-build-create-statement <person>)))

  (dbi-execute-using-connection! raw-conn
   "insert into address (id, city) values (0, 'Leiden')")
  (dbi-execute-using-connection! raw-conn
   "insert into sqlite_sequence (name, seq) values ('address', 0)")
  (dbi-execute-using-connection! raw-conn
   "insert into sqlite_sequence (name, seq) values ('photo', 0)")

  ;; inserts some data
  (let* ((a (make <address> :city "Leiden"))
	 (i (make <photo> :data #vu8(1 2 3 4 5)))
	 (p (make <person> :id 1 :first-names "Takashi"
		  :last-name "Kato" :address a :photo i)))
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
		   (not (eqv? (slot-ref a 'id) (slot-ref a2 'id)))))

    ;; Creates one more record shares address id with other record.
    (let ((p3 (make <person> :id 4 :first-name "Takashi ext"
		    :last-name "Kato" :address a)))
      (test-equal "maquette-insert (4)" 1 (maquette-insert conn p3)))
    )

  (let* ((a (make <address> :city "Den Haag"))
	 (p (make <person> :id 3 :first-names "Takashi Bla"
		  :last-name "Kato" :address a)))
    (test-equal "maquette-insert (3)" 1 (maquette-insert conn p)))
  
  (test-equal "maquette-select (all)" 4
	      (length (maquette-select conn <person>)))
  (test-equal "maquette-select (sub querying)" 3
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
  (test-equal "maquette-delete (2)" 3
	      (maquette-delete conn (make <person> :last-name "Kato")))

  ;; one-to-many
  (dbi-execute-using-connection! 
   raw-conn (ssql->sql (maquette-build-create-statement <customer>)))
  (dbi-execute-using-connection! 
   raw-conn (ssql->sql (maquette-build-create-statement <order>)))
  (let* ((a (car (maquette-select conn <address> '(= city "Leiden"))))
	 (p (make <person> :id 1 :first-names "Takashi"
		  :last-name "Kato" :address a))
	 (customer (make <customer> :who p :id 1))
	 (order1 (make <order> :id 1 :amount 100 :customer customer))
	 (order2 (make <order> :id 2 :amount 150 :customer customer)))
    (slot-set! customer 'order (list order1 order2))
    (test-equal "insert customer" 1 (maquette-insert conn customer))
    (test-equal "select order 1" 2 (length (maquette-select conn <order>)))
    (let ((customer (maquette-select conn <customer>)))
      (test-equal "select customer 1" 1 (length customer))
      (test-equal "select customer order" 2
		  (length (slot-ref (car customer) 'order)))
      (let ((orders (list-sort (lambda (a b)
				 (< (slot-ref a 'id) (slot-ref b 'id)))
			       (slot-ref (car customer) 'order))))
	(test-equal "order detail 1" '(1 100)
		    (list (slot-ref (car orders) 'id)
			  (slot-ref (car orders) 'amount)))
	(test-equal "order detail 2" '(2 150)
		    (list (slot-ref (cadr orders) 'id)
			  (slot-ref (cadr orders) 'amount)))))
    (slot-set! order1 'amount 200)
    (maquette-update conn customer)
    (let* ((customer (maquette-select conn <customer>))
	   (orders (list-sort (lambda (a b)
				(< (slot-ref a 'id) (slot-ref b 'id)))
			      (slot-ref (car customer) 'order))))
      (test-equal "order detail 1" '(1 200)
		  (list (slot-ref (car orders) 'id)
			(slot-ref (car orders) 'amount)))
      (test-equal "order detail 2" '(2 150)
		  (list (slot-ref (cadr orders) 'id)
			(slot-ref (cadr orders) 'amount))))
    ;; SQLite3 allow me to delete records referred by other table.
    ;; so foreign key is just for show...
    ;;(test-error "foreign key constraint" (maquette-delete conn customer))
    (maquette-delete conn customer :cascade? #t)
    (test-assert (null? (maquette-select conn <customer>)))
    (test-assert (null? (maquette-select conn <order>)))
    )

  (maquette-connection-close! conn)
  (when (file-exists? db-file) (guard (e (else #t))(delete-file db-file)))

  )
 (else 
  (display "SKIPPED!" (current-error-port)) (newline (current-error-port))
  (display "Actual database access tests require SQLite3 binding" 
	   (current-error-port))
  (newline (current-error-port))
  ))

(test-end)
