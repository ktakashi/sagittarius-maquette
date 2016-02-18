(import (clos user) (clos core) (maquette tables) (maquette query))

(define-class <address> ()
  ((id :init-keyword :id :primary-key #t
       ;; for testing, we use SQLite3 which doesn't have sequence or stored
       ;; procedure. so make it like this...
       :generator (maquette-generator "insert into address_seq (id) values ((select max(id)+1 from address_seq)); select max(id) from address_seq;"))
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

(define-class <person> ()
  ((id :init-keyword :id :primary-key #t :sql-type 'bigint)
   (first-names :init-keyword :first-names :column-name 'firstNames 
		:sql-type '(varchar 255))
   (last-name :init-keyword :last-name :column-name 'lastName
	      :sql-type '(varchar 50))
   (address :init-keyword :address :foreign-key (list <address> 'id)
	    :column-name 'addressId))
  :metaclass <maquette-table-meta>)

(define-method write-object ((o <person>) out)
  (define (collect-slot o)
    (map (lambda (slot)
	   (let ((name (slot-definition-name slot)))
	     (if (slot-bound? o name)
		 (list name (slot-ref o name))
		 (list name 'unbound)))) (class-slots (class-of o))))
  (format out "#<person ~s>" (collect-slot o)))
