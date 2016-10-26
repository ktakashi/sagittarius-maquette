;;; -*- mode:scheme; coding:utf-8; -*-
;;;
;;; maquette/query/builders.scm - SQL statement builders
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


(library (maquette query builders)
  (export maquette-build-create-statement
	  maquette-build-select-statement
	  maquette-build-insert-statement
	  maquette-build-update-statement
	  maquette-build-delete-statement)
  (import (rnrs)
	  (maquette tables)
	  (maquette query helper)
	  (clos core)
	  (srfi :1 lists)
	  (srfi :2 and-let*)
	  (srfi :26 cut))

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

;;; SELECT
(define (maquette-build-select-statement class :optional (condition #f)
							 (value-handler values))
  (define (->column spec)
    ;; don't load if it's lazy but we want to keep column name
    (cond ((maquette-column-lazy? spec) `(as null ,(maquette-column-name spec)))
	  ((or (maquette-column-one-to-many? spec)
	       (maquette-column-many-to-many? spec)) #f)
	  (else (maquette-column-name spec))))
  `(select ,(filter-map ->column (maquette-table-column-specifications class))
	   (from ,(maquette-table-name class))
	   ,@(if condition
		 `((where ,(build-condition class condition value-handler)))
		 '())))

;;; INSERT
(define (maquette-build-insert-statement class :optional (columns #f))
  (let ((cols (or columns (map car (maquette-table-columns class)))))
    `(insert-into ,(maquette-table-name class)
		  ,cols
		  (values ,(list-tabulate (length cols) (lambda (i) '?))))))

;;; UPDATE
(define (maquette-build-update-statement class columns
					 :optional (condiiton #f)
						   (value-handler values))
  `(update ,(maquette-table-name class)
	   (set! ,@(map (lambda (col) `(= ,col ?)) columns))
	   ,@(if condiiton 
		 `((where ,(build-condition class condiiton value-handler)))
		 '())))

;;; DELETE
(define (maquette-build-delete-statement class :optional (condition #f)
							 (value-handler values))
  `(delete-from ,(maquette-table-name class)
    ,@(if condition
	  `((where ,(build-condition class condition value-handler)))
	  '())))


;; condition must be either #f or a list of the following
;; expression := condition | (or condition ...) | (and condition ...)
;; condition  := (= lhs rhs) | (<> lhs rhs) | < and > ...
;;             | (not expression)
;;             | (or expression ...)
;;             | (and expression ...)
;; utility this can be used for select, update and delete

;; class = table class
;; condition = see above 
;; value-handler = procedure take one argument
(define (build-condition class condition value-handler)
  (define (meta? v)
    (let ((c (class-of v)))
      (and (is-a? c <maquette-table-meta>) c)))
  
  (define (unique? class slot/val)
    (and-let* ((unique (maquette-find-column-specification 
			class maquette-column-unique?))
	       (slot-name (maquette-column-slot-name unique))
	       ( (slot-bound? slot/val slot-name) ))
      unique))
  
  (define (class->ssql ocls slot/val)
    (define (slot->condition slot)
      (let ((n (slot-definition-name slot)))
	(and (slot-bound? slot/val n)
	     (let ((col (maquette-lookup-column-name ocls n)))
	       (value-handler (slot-ref slot/val n))
	       `(= ,col ?)))))
    
    (let* ((spec (maquette-table-primary-key-specification ocls))
	   (column-name (maquette-column-name spec))
	   (slot-name (maquette-column-slot-name spec)))
      (cond ((slot-bound? slot/val slot-name)
	     (value-handler (slot-ref slot/val slot-name))
	     '?)
	    ;; ok build sub query
	    ;; TODO multiple column unique key handling
	    ((unique? class slot/val) =>
	     (lambda (unique)
	       (define unique-column (maquette-column-name unique))
	       (define unique-slot (maquette-column-slot-name unique))
	       (value-handler (slot-ref slot/val unique-slot))
	       `(select (,column-name) (from ,(maquette-table-name ocls))
			(where (= ,unique-column ?)))))
	    (else
	     ;; ok we need to construct query of bound slots
	     `(select (,column-name)
		      (from ,(maquette-table-name ocls))
		      (where (and ,@(filter-map slot->condition
						(class-slots ocls)))))))))
  
  (define (->ssql slot/val)
    (cond ((symbol? slot/val)
	   (or (maquette-lookup-column-name class slot/val) slot/val))
	  ((meta? slot/val) => (cut class->ssql <> slot/val))
	  (else (value-handler slot/val) '?)))

  (case (car condition)
    ((= <> < > <= >=) => (lambda (t) (cons t (map ->ssql (cdr condition)))))
    ((between)
     (let ((col (cadr condition)))
       (build-condition class `(and (<= ,col ,(caddr condition))
				    (<= ,(cadddr condition) ,col))
			value-handler)))
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
    ((not) `(not ,(build-condition class (cadr condition) value-handler)))
    ((or and) =>
     (lambda (t)
       `(,t ,@(map (cut build-condition class <> value-handler)
		   (cdr condition)))))
    (else (error 'maquette-build-select-statement "unknown expression"
		 condition))))
)
