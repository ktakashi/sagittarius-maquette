(import (rnrs)
	(sagittarius)
	(clos user)
	(srfi :64))

;; these are used everywhere so make it separate
;; or maybe separate library would be even better
(include "class-defs.scm")

(test-begin "Maquette - Tables")

(test-equal "table-name" 'person (maquette-table-name <person>))
(test-equal "column-names" 
	    '((id id) (firstNames first-names) (lastName last-name)
	      (addressId address)
	      (photoId photo))
	    (maquette-table-columns <person>))

(test-equal "column-specifications" 
	    `((id id bigint (:primary-key #t))
	      (firstNames first-names (varchar 255))
	      (lastName last-name (varchar 50))
	      (addressId address int (:foreign-key (,<address> id)))
	      (photoId photo int (:foreign-key (,<photo> id))))
	    (maquette-table-column-specifications <person>))

(test-equal "lookup column-name (first-names)" 'firstNames
	    (maquette-lookup-column-name <person> 'first-names))
(test-equal "lookup column-specification (first-names)" 
	    '(firstNames first-names (varchar 255))
	    (maquette-lookup-column-specification <person> 'first-names))

(test-equal "primary key specification (person)" 
	    '(id id bigint (:primary-key #t))
	    (maquette-table-primary-key-specification <person>))

(let ((order (maquette-lookup-column-specification <customer> 'order)))
  (test-assert (is-a? (maquette-column-one-to-many? order) <class>)))


(test-end)
