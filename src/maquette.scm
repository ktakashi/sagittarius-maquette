;;; -*- mode:scheme; coding:utf-8; -*-
;;;
;;; maquette.scm - Maquette an ORM library
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

(library (maquette)
    (export make-maquette-context
	    maquette-context?
	    maquette-max-connection-count

	    maquette-query
	    maquette-save
	    maquette-remove

	    with-maquette-transaction
	    maquette-context-in-transaction?

	    ;; this is needed to create user defined table class
	    <maquette-table-meta>
	    )
    (import (rnrs)
	    (clos core)
	    (sagittarius)
	    (sagittarius object)
	    (maquette context)
	    (maquette connection)
	    (maquette query)
	    (maquette tables))

(define (maquette-query ctx class condition 
			:key (timeout #f) (timeout-value #f))
  (call-with-maquette-connection ctx
    (lambda (conn)
      (maquette-select (maquette-connection-dbi-connection conn) 
		       class condition))
    :timeout timeout :timeout-value timeout-value))

;; *sigh* if we can use MERGE statement...
;; NB: it's not only (text sql) but not a lot of RDBMS support it.
(define (maquette-save ctx obj :key (on-update 'optimistic))
  (call-with-maquette-connection ctx
    (lambda (conn)
      (define dbi-conn (maquette-connection-dbi-connection conn))
      (define class (class-of obj))
      (define primary-key (maquette-table-primary-key-specification class))
      (define id (maquette-column-slot-name primary-key))

      (define (select->insert/update generator?)
	(let ((r (maquette-select dbi-conn class `(= ,id ,(~ obj id)))))
	  (if (null? r)
	      (begin
		(when generator? (set! (~ obj id) (undefined)))
		(maquette-insert dbi-conn obj))
	      (maquette-update dbi-conn obj))))
      (if (slot-bound? obj id)
	  (case on-update
	    ((optimistic) 
	     (if (maquette-column-generator? primary-key)
		 ;; trust it
		 (maquette-update dbi-conn obj)
		 ;; we can't say if this is initial value or not
		 (select->insert/update #f)))
	    ;; alway check existance
	    ((conservative)
	     (select->insert/update (maquette-column-generator? primary-key)))
	    (else (assertion-violation 'maquette-save
		   ":on-update must be 'optimistic or 'conservative"
		   on-update)))
	  (maquette-insert dbi-conn obj))
      obj)))

;; TODO should we check the primary key slot for safety?
(define (maquette-remove ctx template)
  (call-with-maquette-connection ctx
    (lambda (conn)
      (define dbi-conn (maquette-connection-dbi-connection conn))
      (maquette-delete dbi-conn template))))

)
