;;; -*- mode:scheme; coding:utf-8; -*-
;;;
;;; maquette/tables.scm - Table definitnion framework
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

(library (maquette tables)
    (export <maquette-table-meta>
	    maquette-table-name
	    maquette-table-columns
	    maquette-table-column-specifications
	    maquette-lookup-column-name
	    maquette-lookup-column-specification
	    maquette-find-column-specification
	    maquette-table-primary-key-specification

	    maquette-column-name
	    maquette-column-slot-name
	    maquette-column-type
	    ;; predefined predicate
	    maquette-column-primary-key?
	    maquette-column-foreign-key?
	    maquette-column-unique?
	    maquette-column-not-null?
	    maquette-column-default?
	    maquette-column-generator?
	    maquette-column-one-to-many?
	    maquette-column-many-to-many?
	    maquette-column-lazy?
	    
	    maquette-foreign-key-class
	    maquette-foreign-key-slot-name
	    )
    (import (rnrs)
	    (rnrs r5rs)
	    (sagittarius)
	    (srfi :1)
	    (srfi :26)
	    (clos user)
	    (clos core))

(define-class <maquette-table-meta> (<class>)
  (;; actual table name, #f means class name = table name
   ;; must be an symdol (or SSQL delimite identifier if needed)
   (table-name :init-keyword :table-name :init-value #f)
   ;; specifying this makes class have extra slots
   ;; must be alist of column-name and slot-name
   ;; e.g. ((id . identifier) ...) where id = column name
   (columns    :init-keyword :columns    :init-value #f)
   ;; for future, must be cache algorithm class or so
   (cache :init-keyword :cache :init-value #f)
   ;; internal slots
   ;; primary key specification cache
   (primary-key :init-value #f)))

(define-method compute-getter-and-setter ((class <maquette-table-meta>) slot)
  (let ((r (call-next-method))
	(lazy? (slot-definition-option slot :lazy #f))
	(name (slot-definition-name slot)))
    (if lazy?
	(let ((getter (or (car r)
			  (lambda (o) (slot-ref-using-class class o name)))))
	  (cons (lambda (o) (force (getter o))) (cdr r)))
	r)))
(define (strip<> sym)
  (let* ((s (symbol->string sym))
	 (len (string-length s))
	 (start (if (char=? (string-ref s 0) #\<) 1 0))
	 (end (if (char=? (string-ref s (- len 1)) #\>) (- len 1) len)))
    (string->symbol (substring s start end))))

(define (maquette-table-name class)
  (or (slot-ref class 'table-name)
      (let ((r (strip<> (class-name class))))
	(slot-set! class 'table-name r)
	r)))

;; returns 
(define (maquette-table-columns class)
  (filter-map (lambda (s) 
		(and (not (maquette-column-one-to-many? s))
		     (not (maquette-column-many-to-many? s))
		     (take s 2)))
	      (or (slot-ref class 'columns)
		  (maquette-table-column-specifications class))))

;; only needed for create table statement so no cache
(define (slot-definition->column-specification slot)
  (define (call p)
    (if (procedure? p)
	(p)
	p))
  (define (get-constraints slot)
    ;; FIXME looks ugly...
    (let ((primary-key (slot-definition-option slot :primary-key #f))
	  (foreign-key (slot-definition-option slot :foreign-key #f))
	  (unique (slot-definition-option slot :unique #f))
	  (not-null? (slot-definition-option slot :not-null? #f))
	  (default (slot-definition-option slot :default #f))
	  (generator (slot-definition-option slot :generator #f))
	  (one-to-many (call (slot-definition-option slot :one-to-many #f)))
	  (many-to-many (call (slot-definition-option slot :many-to-many #f)))
	  (lazy (call (slot-definition-option slot :lazy #f))))
      `(,@(if primary-key  `((:primary-key  ,primary-key))  '())
	,@(if foreign-key  `((:foreign-key  ,foreign-key))  '())
	,@(if unique       `((:unique       ,unique))       '())
	,@(if not-null?    `((:not-null?    ,not-null?))    '())
	,@(if default      `((:default      ,default))      '())
	,@(if generator    `((:generator    ,generator))    '())
	,@(if one-to-many  `((:one-to-many  ,one-to-many))  '())
	,@(if many-to-many `((:many-to-many ,many-to-many)) '())
	,@(if lazy         `((:lazy         ,lazy))         '())
	)
      ))
    
  (let* ((slot-name (slot-definition-name slot))
	 (column-name (string->symbol
		       (slot-definition-option 
			slot :column-name (symbol->string slot-name))))
	 (sql-type (slot-definition-option slot :sql-type 'int))
	 (constraints (get-constraints slot)))
    (cons* column-name slot-name sql-type constraints)))

(define (maquette-table-column-specifications class)
  (or (slot-ref class 'columns)
      (let ((r (map slot-definition->column-specification (class-slots class))))
	(slot-set! class 'columns r)
	r)))

(define (maquette-lookup-column-name class slot)
  (let loop ((c (maquette-table-columns class)))
    (cond ((null? c)
	   (error 'maquette-lookup-column-name "no slot" class slot))
	  ((eq? (cadar c) slot) (caar c))
	  (else (loop (cdr c))))))

(define (maquette-lookup-column-specification class slot)
  (define columns (maquette-table-column-specifications class))
  (let loop ((slots columns))
    (if (null? slots)
	(error 'maquette-lookup-column-specification "no slot" class slot)
	(let ((s (car slots)))
	  (if (eq? (cadr s) slot)
	      s
	      (loop (cdr slots)))))))

(define (maquette-find-column-specification class pred)
  (let loop ((columns (maquette-table-column-specifications class)))
    (cond ((null? columns) #f)
	  ((pred (car columns)) (car columns))
	  (else (loop (cdr columns))))))

;; accessors
(define (maquette-column-name spec) (car spec))
(define (maquette-column-slot-name spec) (cadr spec))
(define (maquette-column-type spec) (caddr spec))

(define (maquette-column-primary-key? spec) 
  (cond ((assq :primary-key (cddr spec)) => cadr) (else #f)))
(define (maquette-column-foreign-key? spec) 
  (cond ((assq :foreign-key (cddr spec)) => cadr) (else #f)))
(define (maquette-column-unique? spec) 
  (cond ((assq :unique (cddr spec)) => cadr) (else #f)))
(define (maquette-column-not-null? spec) 
  (cond ((assq :not-null? (cddr spec)) => cadr) (else #f)))
(define (maquette-column-default? spec) 
  (cond ((assq :default (cddr spec)) => cadr) (else #f)))
(define (maquette-column-generator? spec) 
  (cond ((assq :generator (cddr spec)) => cadr) (else #f)))
(define (maquette-column-one-to-many? spec) 
  (cond ((assq :one-to-many (cddr spec)) => cadr) (else #f)))
(define (maquette-column-many-to-many? spec) 
  (cond ((assq :many-to-many (cddr spec)) => cadr) (else #f)))
(define (maquette-column-lazy? spec) 
  (cond ((assq :lazy (cddr spec)) => cadr) (else #f)))

(define (maquette-table-primary-key-specification class)
  (define (find-primary-spec)
    (let loop ((spec (maquette-table-column-specifications class)))
      (cond ((null? spec) #f)
	    ((assq :primary-key (cdddr (car spec))) (car spec))
	    (else (loop (cdr spec))))))
  (or (slot-ref class 'primary-key)
      (and-let* ((spec (find-primary-spec)))
	(slot-set! class 'primary-key spec)
	spec)))

(define (maquette-foreign-key-class fkey) (car fkey))
(define (maquette-foreign-key-slot-name fkey) (cadr fkey))
)
