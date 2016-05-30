;;; -*- mode:scheme; coding:utf-8; -*-
;;;
;;; maquette/query.scm - Querying layer
;;;  
;;;   Copyright (c) 2016  Takashi Kato  <ktakashi@ymail.com>
;;;   
;;;   Redistribution and use in source and binary forms, with or without
;;;   modification, are permitted provided that the following conditions
;;;   are met:
;;;   
;;;   1. Redistributions of source code must retain the above copyright
;;;      notice, this list of conditions and the following disclaimer.
;;;  
;;;   2. Redistributions in binary form must reproduce the above copyright
;;;      notice, this list of conditions and the following disclaimer in the
;;;      documentation and/or other materials provided with the distribution.
;;;  
;;;   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
;;;   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
;;;   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
;;;   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
;;;   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
;;;   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
;;;   TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
;;;   PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
;;;   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
;;;   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
;;;   SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
;;;  

(library (maquette query)
    (export maquette-select
	    maquette-insert
	    maquette-update
	    maquette-delete

	    maquette-generator
	    ;; for testing
	    maquette-build-create-statement
	    maquette-build-select-statement
	    maquette-build-insert-statement
	    maquette-build-update-statement
	    maquette-build-delete-statement
	    )
    (import (rnrs)
	    (rnrs mutable-pairs)
	    (sagittarius) ;; for reverse!
	    (sagittarius control) 
	    (clos core)
	    (clos user)
	    (dbi)
	    (maquette tables)
	    (maquette connection)
	    (util hashtables)
	    (text sql)
	    (match)
	    (srfi :1)
	    (srfi :26)
	    (srfi :117))

;;; CREATE
;; more for testing
(define (maquette-build-create-statement class)
  (define specification (maquette-table-column-specifications class))
  (define (parse-spec specification)
    
    (let loop ((specification specification) 
	       (columns '()) 
	       (constraints '()))
      (if (null? specification)
	  (values (reverse! columns) (reverse! constraints))
	  (let ((spec (car specification)))
	    (if (or (maquette-column-one-to-many? spec)
		    (maquette-column-many-to-many? spec))
		(loop (cdr specification) columns constraints)
		(let ((primary-key (maquette-column-primary-key? spec))
		      (foreign-key (maquette-column-foreign-key? spec))
		      (not-null (maquette-column-not-null? spec))
		      (unique (maquette-column-unique? spec))
		      (default (maquette-column-default? spec))
		      (col (maquette-column-name spec))
		      (type (maquette-column-type spec)))
		  (define (build-foreign-key spec)
		    (let ((class (car spec))
			  (key (cadr spec)))
		      `((constraint
			 (foreign-key (,col)
				      (references ,(maquette-table-name class)
						  ,(maquette-lookup-column-name
						    class key)))))))
		  (loop (cdr specification)
			(cons `(,col 
				,type
				,@(if not-null '((constraint not-null)) '())
				,@(if default default '()))
			      columns)
			`(,@(if primary-key 
				      `((constraint (primary-key ,col))) 
				      '())
			  ,@(if foreign-key
				(build-foreign-key foreign-key)
				'())
			  ,@(if unique 
				`((constraint (unique ,col)))
				'())
			  ,@constraints))))))))
  (let-values (((col constraints) (parse-spec specification)))
    `(create-table 
      ,(maquette-table-name class)
      (,@col
       ,@constraints))))

;; condition must be either #f or a list of the following
;; expression := condition | (or condition ...) | (and condition ...)
;; condition  := (= lhs rhs) | (<> lhs rhs) | < and > ...
;;             | (not expression)
;;             | (or expression ...)
;;             | (and expression ...)
;; utility this can be used for select, update and delete

;; class = table class
;; condition = see above 
;; collect? = #f or list-queue
(define (build-condition class condition collect?)
  (define (->ssql slot/val)
    (cond ((symbol? slot/val)
	   (or (maquette-lookup-column-name class slot/val) slot/val))
	  ((is-a? (class-of slot/val) <maquette-table-meta>)
	   ;; TODO we need to walk through the object and build sub query
	   ;;      if primary key isn't set.
	   (let* ((ocls (class-of slot/val))
		  (spec (maquette-table-primary-key-specification ocls)))
	     (define (slot->condition slot)
	       (let ((n (slot-definition-name slot)))
		 (and (slot-bound? slot/val n)
		    (let ((col (maquette-lookup-column-name ocls n)))
		      (when collect?
			(list-queue-add-back! collect? (slot-ref slot/val n)))
		      `(= ,col ?)))))
	     
	     (cond ((slot-bound? slot/val (cadr spec))
		    (when collect?
		      ;; get primary key
		      (list-queue-add-back! collect?
					    (slot-ref slot/val (cadr spec))))
		    '?)
		   ;; ok build sub query
		   ;; TODO multiple column unique key handling
		   ((and-let* ((unique (maquette-find-column-specification 
					class maquette-column-unique?))
			       ( (slot-bound? slot/val (cadr unique)) ))
		      unique) =>
		      (lambda (unique)
			(when collect? 
			  (list-queue-add-back! collect? 
			   (slot-ref slot/val (cadr unique))))
		       `(select (,(car spec)) (from ,(maquette-table-name ocls))
				(where (= ,(car unique) ?)))))
		   (else
		    ;; ok we need to construct query of bound slots
		    `(select (,(car spec)) (from ,(maquette-table-name ocls))
			     (where (and ,@(filter-map slot->condition 
					    (class-slots ocls)))))))))
		    
		 

	  (else (when collect? (list-queue-add-back! collect? slot/val)) '?)))
  (case (car condition)
    ((= <> < > <= >=) => (lambda (t) (cons t (map ->ssql (cdr condition)))))
    ((between)
     (let ((col (cadr condition)))
       (build-condition class `(and (<= ,col ,(caddr condition))
				    (<= ,(cadddr condition) ,col)) collect?)))
    ((in) 
     ;; in needs special treatment if condition value is sub query
     (let ((col (->ssql (cadr condition)))
	   (val (map ->ssql (cddr condition))))
       (let-values (((replacements sub-queries)
		     (partition (lambda (x) (eq? '? x)) val)))
	 (if (null? sub-queries)
	     (list 'in col val)
	     `(and ,@(if (null? replacements)
			 '()
			 `((in ,col ,replacements)))
		   ,@(map (lambda (sub) `(in ,col ,sub)) sub-queries))))))
    ((not) `(not ,(build-condition class (cadr condition) collect?)))
    ((or and) =>
     (lambda (t)
       `(,t ,@(map (cut build-condition class <> collect?) (cdr condition)))))
    (else (error 'maquette-build-select-statement "unknown expression"
		 condition))))

;;; SELECT
(define (maquette-build-select-statement 
	 class condition :optional (collect? #f))
  (define (->column spec)
    ;; don't load if it's lazy but we want to keep column name
    (cond ((maquette-column-lazy? spec) `(as null ,(maquette-column-name spec)))
	  ((or (maquette-column-one-to-many? spec)
	       (maquette-column-many-to-many? spec)) #f)
	  (else (maquette-column-name spec))))
  `(select ,(filter-map ->column (maquette-table-column-specifications class))
	   (from ,(maquette-table-name class))
	   ,@(if condition
		 `((where ,(build-condition class condition collect?)))
		 '())))

;; query = dbi query
;; this is an internal procedure to map query 
;; 
(define (maquette-map-query conn query class delaying loaded)
  (define slots (class-slots class))
  (define slot&columns
    (map (lambda (s) 
	   (cons (slot-definition-name s)
		 (string-foldcase (slot-definition-option s :column-name ""))))
	 slots))

  (define (map-query1 columns q obj)
    (define (find-slot i)
      (define col (vector-ref columns i))
      (let loop ((slots slot&columns))
	(cond ((null? slots) (string->symbol col)) ;; default
	      ((string=? (cdar slots) col) (caar slots))
	      (else (loop (cdr slots))))))
    (define len (vector-length columns))

    (define (create-lazy obj spec slot)
      (define color (maquette-connection-color conn))
      (lambda ()
	(unless (maquette-connection-in-same-session? conn color)
	  (error 'maquette-lazy-load
		 "Lazy loading must be done in the same session" color))
	(let* ((primary-key (maquette-table-primary-key-specification class))
	       (id (slot-ref obj (maquette-column-slot-name primary-key)))
	       (stmt (maquette-connection-prepared-statement conn
		       (ssql->sql
			`(select (,(maquette-column-name spec))
				 (from ,(maquette-table-name class))
				 (where (= ,(maquette-column-name primary-key)
					   ?))))
		       id))
	       (q (dbi-execute-query! stmt)))
	  (let ((v (dbi-fetch! q)))
	    (maquette-connection-close-statement conn q)
	    (if v (vector-ref v 0) '())))))

    (let loop ((i 0))
      (if (= i len)
	  obj ;; TODO cache if the flag is there
	  (let ((slot (find-slot i)))
	    (when (slot-exists? obj slot)
	      (let ((spec (maquette-lookup-column-specification class slot))
		    (v (vector-ref q i)))
		(cond ((hashtable-ref delaying slot #f) =>
		       (lambda (delayed)
			 (case (car delayed)
			   ((foreign)
			    (let* ((s (cdr delayed))
				   (l (vector-ref s 1)))
			      (cond ((assoc v l) =>
				     (lambda (slot)
				       (set-cdr! slot (cons obj (cdr slot)))))
				    (else
				     (vector-set! s 1 
						  (acons v (list obj) l))))))
			   (else (error 'internal "unknown")))))
		      ((and-let* ((fkey (maquette-column-foreign-key? spec))
				  ( (not (hashtable-contains? 
					  loaded (list (car fkey) v)))))
			 fkey)=> 
		       (lambda (key)
			 (hashtable-set! delaying slot 
			   (cons 'foreign `#(,key ((,v ,obj)))))))
		      ((maquette-column-lazy? spec)
		       ;; now we need to make it thunk
		       ;; NB the connection must be in the same session
		       (slot-set! obj slot (create-lazy obj spec slot)))
		      (else (slot-set! obj slot v)))))
	    (loop (+ i 1))))))

  ;; assume columns are mapped to query result properly
  (let* ((columns (vector-map string-foldcase (dbi-columns query))))
    (let loop ((q (dbi-fetch! query)) (r '()))
      (if q
	  (let ((obj (make class)))
	    (loop (dbi-fetch! query) (cons (map-query1 columns q obj) r)))
	  (reverse! r)))))

(define (maquette-select-inner conn class condition loaded)
  (define primary-key (maquette-table-primary-key-specification class))

  (define (handle-foreign-key this-slot value)
    (let* ((key (vector-ref value 0))
	   (values  (vector-ref value 1))
	   (slot (cadr key))
	   (v* (map car values)))
      (let loop ((r (maquette-select-inner conn (car key) 
					   `(in ,slot ,@v*) loaded)))
	  (unless (null? r)
	    (let* ((o (car r))
		   (id (slot-ref o slot)))
	      ;; never heard of foreign key or primary key to be non
	      ;; number but just in case.
	      (cond ((assoc id values) =>
		     (lambda (slot) 
		       (for-each (lambda (this)
				   (slot-set! this this-slot o))
				 (cdr slot))))))
	    (loop (cdr r))))))
  ;; TODO maybe we want to do caching prepared statement?
  (let* ((queue (if condition (list-queue) #f))
	 (ssql (maquette-build-select-statement class condition queue))
	 (stmt (apply maquette-connection-prepared-statement
		      conn (ssql->sql ssql)
		      (if queue (list-queue-list queue) '()))))
    (guard (e (else (maquette-connection-close-statement conn stmt) (raise e)))
      (let* ((q (dbi-execute-query! stmt))
	     (delaying (make-eq-hashtable))
	     (r (maquette-map-query conn q class delaying loaded)))
	;; bad design, this closes prepared statement as well
	;; then how can we reuse it?
	;; To fix this, we need to make all DBD implementations
	;; not to close prepared statement.
	(maquette-connection-close-statement conn q)

	;; now mapping foreign keys
	(hashtable-for-each
	 (lambda (slot value)
	   (case (car value)
	     ((foreign) 
	      (handle-foreign-key slot (cdr value)))
	     (else (error 'maquette-select "unknown"))))
	 delaying)

	;; handling collection if there is
	(unless (null? r)
	  (dolist (spec (maquette-table-column-specifications class))
	    (cond ((maquette-column-one-to-many? spec) =>
		   (lambda (other)
		     (define pslot (maquette-column-slot-name primary-key))
		     (let* ((fkey (find-foreign-key other class))
			    (c (maquette-select-inner conn other
				 `(in ,fkey
				      ,@(map (lambda (this) 
					       (let ((id (slot-ref this pslot)))
						 (hashtable-set! 
						  loaded (list class id) #t)
						 id)) r))
				 loaded)))
		       ;; foreign key slot is mere number (or something else
		       ;; but object)
		       (dolist (this r)
			 (let ((id (slot-ref this pslot)))
			   (slot-set! this
				      (maquette-column-slot-name spec)
				      (filter-map (lambda (r)
						    (and (= (slot-ref r fkey)
							    id)
							 r))
						  c)))))))
		  ;; TODO many-to-one
		  )))
	r))))

(define (maquette-select conn class :optional (condition #f))
  (maquette-select-inner conn class condition (make-equal-hashtable)))

(define (find-foreign-key target this)
  (define specs (maquette-table-column-specifications target))
  (let loop ((specs specs))
    (if (null? specs)
	(error 'maquette-insert 
	       "class referred by :one-to-many keyword must have :foreign-key"
	       target)
	(cond ((and-let* ((fkey (maquette-column-foreign-key? (car specs)))
			  ( (eq? (car fkey) this)))
		 (maquette-column-slot-name (car specs))))
	      (else (loop (cdr specs)))))))
;;; INSERT
(define (maquette-generator expression)
  (define sqls 
    (let ((in (open-string-input-port expression)))
      (let loop ((r '()))
	(let ((sql (read-sql in)))
	  (if (eof-object? sql)
	      (reverse! r)
	      (loop (cons sql r)))))))
  (lambda (object conn)
    (define dbi-conn (maquette-connection-dbi-connection conn))
    (let loop ((sqls sqls))
      (if (null? (cdr sqls))
	  (let* ((q (dbi-execute-query-using-connection! dbi-conn (car sqls)))
		 (v (dbi-fetch! q)))
	    (dbi-close q)
	    (vector-ref v 0))
	  (begin
	    (dbi-execute-using-connection! dbi-conn (car sqls))
	    (loop (cdr sqls)))))))

(define (maquette-build-insert-statement class columns)
  (let ((cols (if (null? columns)
		  (map car (maquette-table-columns class))
		  columns)))
    `(insert-into ,(maquette-table-name class)
		  ,cols
		  (values ,(list-tabulate (length cols) (lambda (i) '?))))))
;; insertion can be done with object without class.
(define (maquette-insert conn object)
  (define class (class-of object))
  (define slots (class-slots class))
  (define primary-key (maquette-table-primary-key-specification class))

  (define (generate-next-id object primary-key)
    (define generator (maquette-column-generator? primary-key))
    (cond ((not (slot-bound? object (cadr primary-key)))
	   (if generator
	       (generator object conn)
	       (error 'maquette-insert 
		      "primary key slot is unbound but no :generator" object)))
	  ;; then you need to set manually
	  ;; otherwise unbound slot error
	  (else 
	   (if generator
	       (error 'maquette-insert
		      ":generator is specified but slot is set" object)
	       (slot-ref object (cadr primary-key))))))

  (define (handle-slots object slots delayed)
    (define (find-primary-key o)
      (define class (class-of o))
      (maquette-table-primary-key-specification class))

    (define (handle-slot slot)
      (and-let* ((slot-name (slot-definition-name slot))
		 ( (slot-bound? object slot-name) )
		 (o (slot-ref object slot-name))
		 (spec (maquette-lookup-column-specification class slot-name)))
	(cond ((is-a? (class-of o) <maquette-table-meta>)
	       ;; ok it's nested class insert if nessessary
	       ;; if this is foreign-key then we need to check
	       ;; the reference columns are filled
	       (let ((column (maquette-column-name spec))
		     (slot   (maquette-column-slot-name spec)))
		 (cond ((maquette-column-foreign-key? spec) =>
			(lambda (fkey)
			  (unless (slot-bound? o (cadr fkey))
			    (maquette-insert conn o))
			  ;; return the inserting value
			  `(,column . ,(slot-ref o (cadr fkey)))))
		       ((find-primary-key o) =>
			(lambda (pkey)
			  (let ((pslot (maquette-column-slot-name pkey)))
			    (unless (slot-bound? o pslot)
			      (maquette-insert conn o))
			    ;; return the interting value
			    `(,column . ,(slot-ref o pslot)))))
		       (else
			(error 'maquette-insert 
			       "Object doesn't have foreign-key nor primary key"
			       o)))))
	      ((pair? o)
	       (cond ((maquette-column-one-to-many? spec) =>
		      (lambda (c)
			(let ((fkey (find-foreign-key c class)))
			  (for-each (lambda (v)
				      (slot-set! v fkey object)
				      (list-queue-add-back! delayed v)) o)
			  #f)))
		     (else
		      (error 'maquette-insert ":many-to-many not supported yet"
			     o))))
	      ((vector? o) (error 'maquette-insert "not supported yet" o))
	      ((maquette-column-primary-key? spec) #f)
	      ;; must be something bindable
	      (else `(,(maquette-column-name spec) . ,o)))))
    (filter-map handle-slot slots))

  (unless primary-key 
    (error 'maquette-insert 
	   "The class of the inserting object doesn't have primary key"
	   class))
  (and-let* ((id (generate-next-id object primary-key))
	     (queue (list-queue))
	     (col&vals (cons (cons (maquette-column-name primary-key) id)
			     (handle-slots object slots queue)))
	     (ssql (maquette-build-insert-statement class (map car col&vals)))
	     (stmt (maquette-connection-prepared-statement
		    conn (ssql->sql ssql))))
    ;; bind parameter
    (let loop ((i 1) (col&vals col&vals))
      (unless (null? col&vals)
	(dbi-bind-parameter! stmt i (cdar col&vals))
	(loop (+ i 1) (cdr col&vals))))
    (let ((r (dbi-execute! stmt)))
      (maquette-connection-close-statement conn stmt)
      (unless (zero? r) (slot-set! object (cadr primary-key) id))
      (unless (list-queue-empty? queue)
	(for-each (cut maquette-insert conn <>) (list-queue-list queue)))
      r)))

;;; UPDATE
(define (maquette-build-update-statement class columns condiiton 
					 :optional (collect? #f))
  `(update ,(maquette-table-name class)
	   (set! ,@(map (lambda (col) `(= ,col ?)) columns))
	   ,@(if condiiton 
		 `((where ,(build-condition class condiiton collect?)))
		 '())))

(define (maquette-update conn object) 
  (define class (class-of object))
  (define primary-key (maquette-table-primary-key-specification class))
  (define pslot (maquette-column-slot-name primary-key))

  (define (collect-columns o q o2m)
    ;; we don't allow to change id, if you want to do it
    ;; do delete -> insert.
    (filter-map 
     (lambda (s)
       (and-let* (( (not (eq? s pslot)) )
		  ( (slot-bound? o s) )
		  (o2 (slot-ref o s))
		  (spec (maquette-lookup-column-specification 
			 class s))
		  (col (maquette-column-name spec)))
	 (cond ((maquette-column-one-to-many? spec)
		(for-each (cut list-queue-add-back! o2m <>) o2)
		#f)
	       ((is-a? (class-of o2) <maquette-table-meta>)
		(cond ((maquette-column-foreign-key? spec) =>
		       (lambda (fkey)
			 (and-let* (( (slot-bound? o2 (cadr fkey)) )
				    (v (slot-ref o2 (cadr fkey))))
			   (list-queue-add-back! q v)
			   col)))
		      (else
		       (error 'maquette-update
			      "The value is not association of given object"
			      o object))))
	       (else
		(list-queue-add-back! q o2)
		col))))
		(map slot-definition-name (class-slots class))))
  (define (create-condition o)
    (let ((pkey-slot (maquette-column-slot-name primary-key)))
      (if (slot-bound? o pkey-slot)
	  `(= ,pkey-slot ,(slot-ref o pkey-slot))
	  ;; ugh
	  `(and ,@(filter-map 
		   (lambda (slot)
		     (and (slot-bound? o slot)
			  `(= ,slot ,(slot-ref o slot))))
		   (map slot-definition-name (class-slots class)))))))
  (let* ((value (list-queue))
	 (one-to-many (list-queue))
	 (columns (collect-columns object value one-to-many))
	 (condition (create-condition object))
	 (ssql (maquette-build-update-statement class columns condition value))
	 (stmt (apply maquette-connection-prepared-statement
		      conn (ssql->sql ssql) 
		      (list-queue-list value))))
    (let ((r (dbi-execute! stmt)))
      (maquette-connection-close-statement conn stmt)
      (for-each (cut maquette-update conn <>) (list-queue-list one-to-many))
      r)))

;;; DELETE
(define (maquette-build-delete-statement class condition
					 :optional (collect? #f))
  `(delete-from ,(maquette-table-name class)
		,@(if condition
		      `((where ,(build-condition class condition collect?)))
		      '())))

(define (maquette-delete conn object :key (cascade? #f))
  (define class (class-of object))
  (define primary-key (maquette-table-primary-key-specification class))
  (define pkey-slot (maquette-column-slot-name primary-key))
  (define (create-condition o)
    (if (slot-bound? o pkey-slot)
	`(= ,pkey-slot ,(slot-ref o pkey-slot))
	;; ugh
	(let ((c (filter-map 
		  (lambda (slot)
		    (let ((spec (maquette-lookup-column-specification
				 class slot)))
		      (and (not (maquette-column-one-to-many? spec))
			   (slot-bound? o slot)
			   `(= ,slot ,(slot-ref o slot)))))
		  (map slot-definition-name (class-slots class)))))
	  (cond ((null? c) #f) ;;what?
		((null? (cdr c)) (car c))
		(else `(and ,@c))))))

  (when cascade?
    (let loop ((specs (maquette-table-column-specifications class)))
      (unless (null? specs)
	(let ((spec (car specs)))
	  (cond ((maquette-column-one-to-many? spec)
		 (let ((o (slot-ref object (maquette-column-slot-name spec))))
		   ;; it must be the list of the same object.
		   (unless (null? o)
		     (let* ((c (class-of (car o)))
			    (t (make c))
			    (fkey (find-foreign-key c class)))
		       (slot-set! t fkey (slot-ref object pkey-slot))
		       (maquette-delete conn t :cascade? #t)))))
		))
	(loop (cdr specs)))))

  (let* ((value (list-queue))
	 (condition (create-condition object))
	 (ssql (maquette-build-delete-statement class condition value))
	 (stmt (apply maquette-connection-prepared-statement
		      conn (ssql->sql ssql) 
		      (list-queue-list value))))
    (let ((r (dbi-execute! stmt)))
      (maquette-connection-close-statement conn stmt)
      r)))

)
