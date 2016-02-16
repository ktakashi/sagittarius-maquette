(import (clos user) (maquette tables))

(define-class <address> ()
  ((id :init-keyword :id :primary-key #t)
   (city :init-keyword :city :sql-type 'varchar :not-null? #t))
  :metaclass <maquette-table-meta>)

(define-class <person> ()
  ((id :init-keyword :id :primary-key #t :sql-type 'bigint)
   (first-names :init-keyword :first-names :column-name 'firstNames 
		:sql-type '(varchar 255))
   (last-name :init-keyword :last-name :column-name 'lastName
	      :sql-type '(varchar 50))
   (address :init-keyword :address :foreign-key (list <address> 'id)
	    :column-name 'addressId))
  :metaclass <maquette-table-meta>)

