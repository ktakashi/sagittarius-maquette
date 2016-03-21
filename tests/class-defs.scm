(import (clos user) (clos core) (maquette tables) (maquette query) 
	(srfi :18))

(define-class <sqlite_sequence> ()
  ((name :sql-type 'varchar)
   (seq :sql-type 'integer))
  :metaclass <maquette-table-meta>)
(define generator
  (let ((mutex (make-mutex)))
    (lambda (object conn)
      ;; unfortunately we need global lock.
      (mutex-lock! mutex)
      (guard (e (else (mutex-unlock! mutex) (raise e)))
	(let* ((table (symbol->string (maquette-table-name (class-of object))))
	       (r (maquette-select conn <sqlite_sequence> `(= name ,table))))
	  (when (null? r)
	    ;; mis configuration
	    (mutex-unlock! mutex)
	    (assertion-violation 'sqlite-generator
				 "sqlite_sequence doesn't contain the table"
				 table))
	  (let ((seq (slot-ref (car r) 'seq)))
	    (mutex-unlock! mutex)
	    (+ seq 1)))))))

(define-class <address> ()
  ((id :init-keyword :id :primary-key #t
       :generator generator)
   (city :init-keyword :city :sql-type 'varchar :not-null? #t))
  :metaclass <maquette-table-meta>)

(define-method write-object ((o <address>) out)
  (define (collect-slot o)
    (map (lambda (slot)
	   (let ((name (slot-definition-name slot)))
	     (if (slot-bound? o name)
		 (list name (slot-ref o name))
		 (list name 'unbound)))) (class-slots (class-of o))))
  (format out "#<address ~s>" (collect-slot o)))

(define-class <photo> ()
  ((id :init-keyword :id :primary-key #t :generator generator)
   (data :init-keyword :data :lazy #t :sql-type 'blob))
  :metaclass <maquette-table-meta>)

(define-method write-object ((o <photo>) out)
  (define (collect-slot o)
    (map (lambda (slot)
	   (let ((name (slot-definition-name slot)))
	     (if (slot-bound? o name)
		 (list name (slot-ref o name))
		 (list name 'unbound)))) (class-slots (class-of o))))
  (format out "#<photo ~s>" (collect-slot o)))

(define-class <person> ()
  ((id :init-keyword :id :primary-key #t :sql-type 'bigint)
   (first-names :init-keyword :first-names :column-name "firstNames"
		:sql-type '(varchar 255))
   (last-name :init-keyword :last-name :column-name "lastName"
	      :sql-type '(varchar 50))
   (address :init-keyword :address :foreign-key (list <address> 'id)
	    :column-name "addressId")
   (photo :init-keyword :photo :foreign-key (list <photo> 'id)
	  :column-name "photoId"))
  :metaclass <maquette-table-meta>)

(define-method write-object ((o <person>) out)
  (define (collect-slot o)
    (map (lambda (slot)
	   (let ((name (slot-definition-name slot)))
	     (if (slot-bound? o name)
		 (list name (slot-ref o name))
		 (list name 'unbound)))) (class-slots (class-of o))))
  (format out "#<person ~s>" (collect-slot o)))

(define-class <customer> ()
  ((id :init-keyword :id :primary-key #t :sql-type 'bigint)
   (who :init-keyword :who :foreign-key (list <person> 'id))
   ;; one-to-many can have class or thunk returns class
   (order :init-keyword :customer :one-to-many (lambda () <order>)))
  :metaclass <maquette-table-meta>)

(define-class <order> ()
  ((id :init-keyword :id :primary-key #t :sql-type 'bigint)
   (amount :init-keyword :amount :sql-type 'bigint)
   (customer :init-keyword :customer :foreign-key (list <customer> 'id)))
  :metaclass <maquette-table-meta>
  :table-name 'order_table)
