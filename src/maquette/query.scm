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
	    ;;maquette-update
	    ;;maquette-delete

	    ;; for testing
	    maquette-build-create-statement
	    maquette-build-select-statement
	    )
    (import (rnrs)
	    (sagittarius) ;; for reverse!
	    (clos core)
	    (clos user)
	    (dbi)
	    (maquette tables)
	    (text sql)
	    (match))
;; query = dbi query
;; this is an internal procedure to map query 
;; 
(define (maquette-map-query query class)
  (define slots (class-slots class))

  (define (map-query1 columns q obj)
    (define (find-slot i)
      (define col (vector-ref columns i))
      (let loop ((slots slots))
	(cond ((null? slots) (string->symbol col)) ;; default
	      ((string=? (slot-definition-option (car slots)
						 :column-name "") col)
	       (slot-definition-name (car slots)))
	      (else (loop (cdr slots))))))
    (define len (vector-length columns))
    (let loop ((i 0))
      (if (= i len)
	  obj ;; TODO cache if the flag is there
	  (let ((slot (find-slot i)))
	    (when (slot-exists? obj slot)
	      ;; TODO - foreign key (nested object)
	      ;;      - collections (one to many)
	      (slot-set! obj slot (vector-ref q i)))
	    (loop (+ i 1))))))
  ;; assume columns are mapped to query result properly
  (let* ((columns (dbi-columns query)))
    (let loop ((q (dbi-fetch! query)) (r '()))
      (if q
	  (let ((obj (make class)))
	    (loop (dbi-fetch! query) (cons (map-query1 columns q obj) r)))
	  (reverse! r)))))

(define (maquette-build-create-statement class)
  (define specification (maquette-table-column-specifications class))
  (define (parse-spec specification)
    
    (let loop ((specification specification) 
	       (columns '()) 
	       (constraints '()))
      (if (null? specification)
	  (values (reverse! columns) (reverse! constraints))
	  (match (car specification)
	    ((col _ type constraint ...)
	     (let ()
	       (define (build-foreign-key spec)
		 (let ((class (car spec))
		       (key (cadr spec)))
		   `((constraint
		      (foreign-key (,col)
				   (references ,(maquette-table-name class)
					       ,(maquette-lookup-column-name
						 class key)))))))
	       ;; boolean get
	       (define (get key)
		 (cond ((assq key constraint) => 
			(lambda (slot) (cond ((cadr slot)) (else '()))))
		       (else '())))
	       (define (getb key r)
		 (if (null? (get key)) '() r))
	       (loop 
		(cdr specification)
		(cons `(,col ,type
			     ,@(getb :not-null? '((constraint not-null)))
			     ,@(get :default))
		      columns)
		(let ((primary (getb :primary-key 
				     `((constraint (primary-key ,col)))))
		      (foreign (cond ((get :foreign-key) => 
				      (lambda (s)
					(if (null? s) 
					    '()
					    (build-foreign-key s))))
				     (else ())))
		      (unique (getb :unique `((constraint (unique ,col))))))
		  ;; FIXME duplicate primary key won't be detected
		  `(,@primary ,@foreign ,@unique ,@constraints)))))))))
  (let-values (((col constraints) (parse-spec specification)))
    `(create-table 
      ,(maquette-table-name class)
      (,@col
       ,@constraints))))

(define (maquette-build-select-statement class condition)
  `(select ,(map car (maquette-table-columns class))
	   (from ,(maquette-table-name class))
	   ;; TODO condition
	   ,@(if condition '() ())))

;; call dbi-bind-parameter!
(define (apply-condition stmt condition) #f)

(define (maquette-select conn class :optional (condition #f))
  ;; TODO maybe we want to do caching prepared statement?
  (let ((stmt (dbi-prepare conn 
			   (maquette-build-select-statement class condition))))
    (when condition (apply-condition stmt condition))
    (let ((q (dbi-execute! stmt)))
      (let ((r (maquette-map-query q class)))
	;; bad design, this closes prepared statement as well
	;; then how can we reuse it?
	;; To fix this, we need to make all DBD implementations
	;; not to close prepared statement.
	(dbi-close q)
	r))))

;; insertion can be done with object without class.
(define (maquette-insert conn object)
  (define class (class-of object))
  #t)

)
