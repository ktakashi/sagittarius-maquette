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
	    maquette-lookup-column-name)
    (import (rnrs)
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
   (cache :init-keyword :cache :init-value #f)))

(define (strip<> sym)
  (let* ((s (symbol->string sym))
	 (len (string-length s))
	 (start (if (char=? (string-ref s 0) #\<) 1 0))
	 (end (if (char=? (string-ref s (- len 1)) #\>) (- len 1) len)))
    (string->symbol (substring s start end))))

;; TODO should we store them into class itself for performance?
;;      since these are more static things so it doesn't have to
;;      be computed each time.
(define (maquette-table-name class)
  (or (slot-ref class 'table-name)
      (strip<> (class-name class))))

;; returns 
(define (slot-definition->columns slot)
  (let* ((slot-name (slot-definition-name slot))
	 (column-name (slot-definition-option 
		       slot :column-name slot-name)))
    (cons column-name slot-name)))
(define (maquette-table-columns class)
  (or (slot-ref class 'columns)
      (map slot-definition->columns (class-slots class))))

;; only needed for create table statement so no cache
(define (maquette-table-column-specifications class)
  (define (get-constraints slot)
    (let ((primary-key (slot-definition-option slot :primary-key #f))
	  (foreign-key (slot-definition-option slot :foreign-key #f))
	  (unique (slot-definition-option slot :unique #f))
	  (not-null? (slot-definition-option slot :not-null? #f))
	  (default (slot-definition-option slot :default #f)))
      `((:primary-key ,primary-key)
	(:foreign-key ,foreign-key)
	(:unique ,unique)
	(:not-null? ,not-null?)
	(:default ,default))))
  (map (lambda (slot)
	 (let* ((slot-name (slot-definition-name slot))
		(column-name (slot-definition-option 
			      slot :column-name slot-name))
		(sql-type (slot-definition-option slot :sql-type 'int))
		(constraints (get-constraints slot)))
	   (cons* column-name slot-name sql-type constraints)))
       (class-slots class)))

(define (maquette-lookup-column-name class slot)
  (let loop ((slots (class-slots class)))
    (cond ((null? slot) 
	   (error 'maquette-lookup-column-name "no slot" class slot))
	  ((eq? (slot-definition-name (car slots)) slot)
	   (car (slot-definition->columns (car slots))))
	  (else (loop (cdr slots))))))

)
