;;; -*- mode:scheme; coding:utf-8; -*-
;;;
;;; maquette/cotnext.scm - Context for Maquette
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

(library (maquette context)
    (export make-maquette-context
	    maquette-context-release!
	    maquette-context?
	    maquette-max-connection-count
	    call-with-maquette-connection
	    with-maquette-transaction
	    maquette-context-in-transaction?
	    ;; low level
	    maquette-get-connection
	    maquette-return-connection)
    (import (rnrs)
	    (clos user)
	    (srfi :39)
	    (sagittarius object)
	    (maquette connection))

;; Maquette context
;; A context manages a connection pool. All high level APIs shall take
;; a context to operate database. Users may import low level libraries
;; to do some low level operations such as generating SQL or creating
;; an extra abstraction layer (which should be provided by high level
;; APIs though).
;;
;; Why context? (future note for me) 
;; maquette-get-connection and maquette-return-connection do exactly
;; the same as connection pool operations. Then why do we need context
;; layer?
;; Because we may want to do more on this layer other than retrieving
;; conenctios. Showing connection pool directly to users makes less
;; extensibility. It's not only for not but also for future. (not sure
;; what would be the future yet.)
(define-class <maquette-context> ()
  ((pool :init-keyword :pool) ;; connection pool
   (in-transaction? :init-value #f
		    :reader maquette-context-in-transaction?)
   ;; TODO more?
   ))


;; Combination of with-maquette-transaction and
;; call-with-maquette-connection uses 2 or more connections in one
;; transaction and causes an error on DBI. To avoid it we need to make
;; sure that using the same connection in the same transaction.
;;
;; FIXME ugly
(define *current-connection* (make-parameter #f))

(define (maquette-context? o) (is-a? o <maquette-context>))

(define (maquette-context-release! ctx)
  (maquette-connection-pool-release! (~ ctx 'pool)))

(define (maquette-max-connection-count ctx)
  (maquette-connection-pool-max-connection-count (~ ctx 'pool)))

(define (make-maquette-context dsn :key (max-connection 10)
			       :allow-other-keys opts)
  (unless (> max-connection 0)
    (assertion-violation 'make-maquette-context
			 ":max-connection must be non zero positive integer"
			 max-connection))
  (let ((pool (apply make-maquette-connection-pool max-connection dsn opts)))
    (make <maquette-context> :pool pool)))

(define (call-with-maquette-connection ctx proc 
				       :key (timeout #f) (timeout-value #f))
  (let ((c (*current-connection*)))
    (if c
	(proc c)
	(call-with-available-connection (~ ctx 'pool) 
	  (lambda (conn)
	    (parameterize ((*current-connection* conn)) (proc conn)))
	  timeout timeout-value))))

(define-syntax with-maquette-transaction
  (syntax-rules ()
    ((_ ctx expr ...)
     (let ((c ctx))
       (dynamic-wind
	   (lambda () (set! (~ c 'in-transaction?) #t))
	   (lambda () 
	     (call-with-maquette-connection c
	       (lambda (conn) 
		 (with-maquette-connection-transaction conn 
		   (let ((r (begin expr ...)))
		     ;; commit if there's no problem
		     (maquette-connection-commit! conn)
		     r)))))
	   (lambda () (set! (~ c 'in-transaction?) #f)))))))

;; Low level API
;; Maybe we shouldn't show this at all?
(define (maquette-get-connection ctx . opt)
  (apply maquette-connection-pool-get-connection (~ ctx 'pool) opt))
(define (maquette-return-connection ctx conn)
  (maquette-connection-pool-return-connection (~ ctx 'pool) conn))

)
