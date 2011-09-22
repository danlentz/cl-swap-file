(in-package :common-lisp-user)

;;(require :unit-test)

(defpackage swap-file
  (:use :cl) ;; :unit-test
  (:shadow #:open #:close)
  (:export #:create
           #:flush
           #:rollback
           #:create-block
           #:open-block
           #:unlink-block
           #:create-block-stream
           #:open-block-stream
           #:disk-block-offset
           #:set-file-position-after-header
           #:swap-file-stream
           #:with-open-block-stream
           #:write-uint32-to-disk
           #:open
           #:close
           #:swap-file
           #:format-swap-file
           #:format-data
           #:format-comment
           #:format-octet))


