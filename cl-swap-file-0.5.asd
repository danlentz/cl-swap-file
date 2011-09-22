(in-package :common-lisp-user)

(defpackage :swap-file.system
  (:use :asdf :cl)
  (:export #:cl-swap-file-0.5))

(in-package :swap-file.system)

(defsystem cl-swap-file-0.5
   :name "Common Lisp swap file."
   :description "Swap file is a fixed block size storage."
   :version "0.5"
   :author "Sami Makinen <sami.o.makinen@gmail.com>"
   :components ((:file "package") (:file "swap-file" :depends-on ("package"))
                (:file "block-stream" :depends-on ("swap-file"))
                (:file "debug" :depends-on ("swap-file"))
                (:module "vendor" :components ((:file "lisp-unit")))
                (:module "test" :depends-on ("swap-file" "vendor") :components
                 ((:module "unit" :components ((:file "swap-file"))))))
   :depends-on (:trivial-garbage :cl-wal-0.4 :cl-binary-file-0.4))
