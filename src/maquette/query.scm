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
	    (maquette query builders)
	    (maquette query helper)
	    (util hashtables)
	    (text sql)
	    (match)
	    (srfi :1)
	    (srfi :26)
	    (srfi :45) ;; avoid to use (scheme lazy)...
	    (srfi :117))


(define (list-queue-value-handler queue)
  (lambda (value) (list-queue-add-back! queue value)))
;; query = dbi query
;; this is an internal procedure to map query 
(define (maquette-map-query conn query class loaded)
  (define delaying (make-eq-hashtable))
  (define slot&columns
    (map (lambda (s) 
	   (cons (slot-definition-name s)
		 (string-foldcase (slot-definition-option s :column-name ""))))
	 (class-slots class)))

  (define (map-query1 columns q obj)
    (define (find-slot i)
      (define col (vector-ref columns i))
      (let loop ((slots slot&columns))
	(cond ((null? slots) (string->symbol col)) ;; default
	      ((string=? (cdar slots) col) (caar slots))
	      (else (loop (cdr slots))))))
    
    (define (create-lazy obj spec slot)
      (define color (maquette-connection-color conn))
      (lazy
       (begin
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
	     (if v (vector-ref v 0) '()))))))
    ;; the foreign key of these objects are resolved later
    (define (push-delaying-object! delayed v)
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
	(else (error 'internal "unknown"))))

    (define (non-loaded-foreign-key? spec v)
      (and-let* ((fkey (maquette-column-foreign-key? spec))
		 (c (maquette-foreign-key-class fkey))
		 ( (not (hashtable-contains? loaded (list c v))) ))
	fkey))

    (dotimes (i (vector-length columns) obj)
      (and-let* ((slot (find-slot i))
		 ( (slot-exists? obj slot) ))
	(let ((spec (maquette-lookup-column-specification class slot))
	      (v (vector-ref q i)))
	  (cond ((hashtable-ref delaying slot #f) =>
		 (cut push-delaying-object! <> v))
		((non-loaded-foreign-key? spec v) =>
		 ;; set to delaying
		 (lambda (key)
		   (hashtable-set! delaying slot 
				   (cons 'foreign `#(,key ((,v ,obj)))))))
		((maquette-column-lazy? spec)
		 ;; now we need to make it promise
		 ;; NB the connection must be in the same session
		 (slot-set! obj slot (create-lazy obj spec slot)))
		(else (slot-set! obj slot v)))))))

  ;; assume columns are mapped to query result properly
  (let ((columns (vector-map string-foldcase (dbi-columns query))))
    (values
     (dbi-query-map query (lambda (q) (map-query1 columns q (make class))))
     delaying)))

(define (maquette-select-inner conn class condition loaded)
  (define primary-key (maquette-table-primary-key-specification class))

  (define (handle-foreign-key this-slot value)
    (let* ((key (vector-ref value 0))
	   (values  (vector-ref value 1))
	   (slot (maquette-foreign-key-slot-name key))
	   (v* (map car values)))
      (dolist (o (maquette-select-inner conn (maquette-foreign-key-class key)
					`(in ,slot ,@v*) loaded))
	(let ((id (slot-ref o slot)))
	  ;; never heard of foreign key or primary key to be non
	  ;; number but just in case.
	  (cond ((assoc id values) =>
		 (lambda (fkey-id&objs) 
		   (for-each (lambda (this) (slot-set! this this-slot o))
			     (cdr fkey-id&objs)))))))))

  (define (handle-foreign-keys delayed)
    (hashtable-for-each (lambda (slot value)
			  (case (car value)
			    ((foreign) 
			     (handle-foreign-key slot (cdr value)))
			    (else (error 'maquette-select "unknown"))))
			delayed))
  
  (define (handle-collections r)
    (define (construct-condition fkey pslot)
      `(in ,fkey
	   ,@(map (lambda (this) 
		    (let ((id (slot-ref this pslot)))
		      (hashtable-set! loaded (list class id) #f)
		      id)) r)))

    (dolist (spec (maquette-table-column-specifications class))
      (cond ((maquette-column-one-to-many? spec) =>
	     (lambda (other)
	       (define pslot (maquette-column-slot-name primary-key))
	       (let* ((fkey (find-foreign-key other class))
		      (c (maquette-select-inner conn other
						(construct-condition fkey pslot)
						loaded)))
		 (define (foreign? r id) (and (equal? (slot-ref r fkey) id) r))
		 ;; foreign key slot is mere number (or something else
		 ;; but object)
		 (dolist (this r)
		   (let ((id (slot-ref this pslot)))
		     (slot-set! this
				(maquette-column-slot-name spec)
				(filter-map (cut foreign? <> id) c)))))))
	    ;; TODO many-to-one
	    )))

  (let* ((queue (if condition (list-queue) #f))
	 (handler (list-queue-value-handler queue))
	 (ssql (maquette-build-select-statement class condition handler))
	 (stmt (apply maquette-connection-prepared-statement
		      conn (ssql->sql ssql)
		      (if queue (list-queue-list queue) '()))))
    (guard (e (else (maquette-connection-close-statement conn stmt) (raise e)))
      (let ((q (dbi-execute-query! stmt)))
	(define-values (r delayed) (maquette-map-query conn q class loaded))
	;; bad design, this closes prepared statement as well
	;; then how can we reuse it?
	;; To fix this, we need to make all DBD implementations
	;; not to close prepared statement.
	;; now mapping foreign keys
	(handle-foreign-keys delayed)
	;; handling collection if there is
	(unless (null? r) (handle-collections r))
	r))))

;;; SELECT
(define (maquette-select conn class :optional (condition #f))
  (maquette-select-inner conn class condition (make-equal-hashtable)))

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


;; UPDATE
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
	 (handler (list-queue-value-handler value))
	 (one-to-many (list-queue))
	 (columns (collect-columns object value one-to-many))
	 (condition (create-condition object))
	 (ssql (maquette-build-update-statement class columns
						condition handler))
	 (stmt (apply maquette-connection-prepared-statement
		      conn (ssql->sql ssql) 
		      (list-queue-list value))))
    (let ((r (dbi-execute! stmt)))
      (maquette-connection-close-statement conn stmt)
      (for-each (cut maquette-update conn <>) (list-queue-list one-to-many))
      r)))

;;; DELETE
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
	 (handler (list-queue-value-handler value))
	 (condition (create-condition object))
	 (ssql (maquette-build-delete-statement class condition handler))
	 (stmt (apply maquette-connection-prepared-statement
		      conn (ssql->sql ssql) 
		      (list-queue-list value))))
    (let ((r (dbi-execute! stmt)))
      (maquette-connection-close-statement conn stmt)
      r)))

)
