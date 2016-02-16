(import (rnrs)
	(sagittarius)
	(srfi :64))

;; these are used everywhere so make it separate
;; or maybe separate library would be even better
(include "class-defs.scm")

(test-begin "Maquette - Tables")

(test-equal "table-name" 'person (maquette-table-name <person>))
(test-equal "column-names" 
	    '((id . id) (firstNames . first-names) (lastName . last-name)
	      (addressId . address))
	    (maquette-table-columns <person>))

(test-end)
