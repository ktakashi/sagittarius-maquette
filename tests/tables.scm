(import (rnrs)
	(sagittarius)
	(srfi :64))

;; these are used everywhere so make it separate
;; or maybe separate library would be even better
(include "class-defs.scm")

(test-begin "Maquette - Tables")

(test-equal "table-name" 'person (maquette-table-name <person>))
(test-equal "column-names" 
	    '((id id) (firstNames first-names) (lastName last-name)
	      (addressId address))
	    (maquette-table-columns <person>))

(test-equal "column-specifications" 
	    `((id id bigint (:primary-key #t))
	      (firstNames first-names (varchar 255))
	      (lastName last-name (varchar 50))
	      (addressId address int (:foreign-key (,<address> id))))
	    (maquette-table-column-specifications <person>))

(test-equal "lookup column-name (first-names)" 'firstNames
	    (maquette-lookup-column-name <person> 'first-names))
(test-equal "lookup column-specification (first-names)" 
	    '(firstNames first-names (varchar 255))
	    (maquette-lookup-column-specification <person> 'first-names))

(test-equal "primary key specification (person)" 
	    '(id id bigint (:primary-key #t))
	    (maquette-table-primary-key-specification <person>))

(test-end)
