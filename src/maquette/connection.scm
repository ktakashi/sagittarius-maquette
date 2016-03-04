;;; -*- mode:scheme; coding:utf-8; -*-
;;;
;;; maquette/connection.scm - Connection and connection pool
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

(library (maquette connection)
    (export make-maquette-connection maquette-connection?
	    maquette-connection-dbi-connection
	    maquette-connection-connect!
	    maquette-connection-open?
	    maquette-connection-close!
	    maquette-connection-reconnect!
	    open-maquette-connection
	    maquette-connection-start-transaction!
	    maquette-connection-commit!
	    maquette-connection-rollback!
	    maquette-connection-prepared-statement
	    maquette-connection-close-statement

	    ;; connection pool
	    make-maquette-connection-pool maquette-connection-pool?
	    maquette-connection-pool-get-connection
	    maquette-connection-pool-return-connection
	    maquette-connection-pool-release!
	    maquette-connection-pool-re-pool!
	    maquette-connection-pool-connection-available?
	    maquette-connection-pool-available-connection-count
	    maquette-connection-pool-max-connection-count

	    ;; transaction
	    with-maquette-connection-transaction
	    call-with-available-connection
	    )
    (import (rnrs)
	    (clos user)
	    (sagittarius object)
	    (util concurrent)
	    (srfi :18)
	    (srfi :26)
	    (dbi))

(define-class <maquette-connection> ()
  ((dbi-connection :init-keyword :dbi-connection :init-value #f
		   :reader maquette-connection-dbi-connection)
   ;; for reconnect
   (dsn :init-keyword :dsn)
   (options :init-keyword :options)
   ;; for future...
   (cached-statement :init-value #f)
   ))
(define (maquette-connection? o) (is-a? o <maquette-connection>))
(define (make-maquette-connection dsn
				  :key (cache-size #f)
				  :allow-other-keys opt)
  (define (cache-creation conn)
    (lambda (sql)
      (let ((dbi-connection (maquette-connection-dbi-connection conn)))
	(dbi-prepare dbi-connection sql))))

  (make <maquette-connection> :dsn dsn :options opt))
(define (maquette-connection-open? conn)
  (is-a? (maquette-connection-dbi-connection conn) <dbi-connection>))

(define (maquette-connection-connect! conn)
  (unless (maquette-connection-open? conn)
    (set! (~ conn 'dbi-connection)
	  ;; it's better to have :auto-commit option enabled
	  ;; but we can't assume it's supported by DBD.
	  (apply dbi-connect (~ conn 'dsn) (~ conn 'options))))
  conn)

(define (maquette-connection-close! conn)
  (when (~ conn 'dbi-connection)
    (dbi-close (maquette-connection-dbi-connection conn)))
  (set! (~ conn 'dbi-connection) #f)
  conn)

(define (maquette-connection-reconnect! c)
  (when (~ c 'dbi-connection) (maquette-connection-close! c))
  (maquette-connection-connect! c))

;; convenient procedure
(define (open-maquette-connection dsn . opt)
  (maquette-connection-connect! (apply make-maquette-connection dsn opt)))
(define (maquette-connection-start-transaction! c)
  (dbi-execute-using-connection! (~ c 'dbi-connection) "BEGIN")
  c)
(define (maquette-connection-commit! c) (dbi-commit! (~ c 'dbi-connection)) c)
(define (maquette-connection-rollback! c) 
  (dbi-rollback! (~ c 'dbi-connection)) 
  c)

(define (maquette-connection-prepared-statement c sql . opt)
  (unless (maquette-connection-open? c)
    (assertion-violation 'maquette-connection-prepared-statement
			 "connection is closed" c))
  (let ((stmt (dbi-prepare (maquette-connection-dbi-connection c) sql)))
    (let loop ((i 1) (ps opt))
      (unless (null? ps)
	(dbi-bind-parameter! stmt i (car ps))
	(loop (+ i 1) (cdr ps))))
    stmt))

(define (maquette-connection-close-statement c stmt)
  (unless (~ c 'cached-statement) (dbi-close stmt)))

;;; connection pool
;; We do very simple connection pooling here. The basic strategy is
;; that using shared-queue to manage connection retrieval and returning.
(define-class <maquette-connection-pool> ()
  ((pool :init-keyword :pool) ;; shared-queue
   (connections :init-keyword :connections) ;; actual connections
   ;; for re-pool
   (max-connection :init-keyword :max-connection
		   :reader maquette-connection-pool-max-connection-count)
   (dsn :init-keyword :dsn)
   (options :init-keyword :options)
   (lock :init-form (make-mutex))))

(define (init-connections max-connection dsn options)
  (define (make-connections n)
    (let loop ((i 0) (r '())) 
      (if (= i n) 
	  r
	  (loop (+ i 1) 
		(cons (apply open-maquette-connection dsn options) r)))))
  (let ((sq (make-shared-queue))
	(conns (make-connections max-connection)))
    (for-each (cut shared-queue-put! sq <>) conns)
    (values sq conns)))
(define (make-maquette-connection-pool max-connection dsn . options)
  (let-values (((sq conns) (init-connections max-connection dsn options)))
    (make <maquette-connection-pool> :pool sq :connections conns
	  :dsn dsn :options options :max-connection max-connection)))

(define (maquette-connection-pool? o) (is-a? o <maquette-connection-pool>))

(define (maquette-connection-pool-get-connection cp . opt)
  (let ((r (apply shared-queue-get! (~ cp 'pool) opt)))
    (if (maquette-connection? r)
	(maquette-connection-connect! r)
	r)))

(define (maquette-connection-pool-return-connection cp conn)
  (unless (memq conn (~ cp 'connections))
    (assertion-violation 'maquette-connection-pool-return-connection
			 "not a managed connection" conn))
  ;; avoid duplicate connection to be pushed
  ;; so returning is a bit more expensive than retrieving
  (mutex-lock! (~ cp 'lock))
  ;; once it's returned then at least we need to rollback the 
  ;; transaction.
  (guard (e (else #t)) (maquette-connection-rollback! conn))
  (unless (shared-queue-find (~ cp 'pool) (lambda (e) (eq? e conn)))
    ;; if the connection is closed, then retriever makes sure the connectivity
    ;; so simply push
    (shared-queue-put! (~ cp 'pool) conn))
  (mutex-unlock! (~ cp 'lock))
  cp)

(define (maquette-connection-pool-connection-available? cp)
  (let ((pool (~ cp 'pool)))
    (and pool (not (shared-queue-empty? pool)))))

(define (maquette-connection-pool-available-connection-count cp)
  (let ((pool (~ cp 'pool)))
    (if pool 
	(shared-queue-size pool)
	0)))

;; release all connection
;; TODO should we raise an error if managed connection(s) are used?
(define (maquette-connection-pool-release! cp)
  ;; closes all connection
  (for-each maquette-connection-close! (~ cp 'connections))
  (set! (~ cp 'pool) #f)
  (set! (~ cp 'connections) '())
  cp)

;; Should we provide this?
(define (maquette-connection-pool-re-pool! cp)
  (unless (~ cp 'pool) 
    (let-values (((sq conns) (init-connections (~ cp 'max-connection)
					       (~ cp 'dsn) (~ cp 'options))))
      (set! (~ cp 'pool) sq)
      (set! (~ cp 'connections) conns)))
  cp)

;; High level APIs
;; this would block. it is users' responsibility to check if there's an
;; available connection.
(define (call-with-available-connection cp proc . opt)
  (let ((conn #f))
    (dynamic-wind
	(lambda () 
	  (set! conn (apply maquette-connection-pool-get-connection cp opt)))
	(lambda () (if (maquette-connection? conn) (proc conn) conn))
	(lambda () 
	  (when (maquette-connection? conn)
	    (maquette-connection-pool-return-connection cp conn))))))

;; transaction
(define-syntax with-maquette-connection-transaction
  (syntax-rules ()
    ((_ conn expr ...)
     (let ((c conn))
       (dynamic-wind
	   (lambda () 
	     ;; some DBD throws an error if there's already a transaction.
	     (guard (e (else #t)) (maquette-connection-start-transaction! c)))
	   (lambda () expr ...)
	   (lambda ()
	     ;; some DBD throws an error if there's no transaction.
	     (guard (e (else #t)) (maquette-connection-rollback! c))))))))
	       


)
