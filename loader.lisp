(in-package :common-lisp-user)

(eval-when (:load-toplevel :compile-toplevel :execute)
  (defun load-time-dir ()
    (make-pathname :directory (pathname-directory *load-pathname*)))
  
  (defun parent-dir (dir)
    (make-pathname :directory (butlast (pathname-directory dir))))
  
  (defun append-dir (parent dir)
    (make-pathname :directory (append (pathname-directory parent) (cdr (pathname-directory dir)))))
  
  (defun make-systems-dir ()
    (append-dir (parent-dir (load-time-dir)) (make-pathname :directory ".systems"))))

(defvar *systems-dir* (make-systems-dir))

(pushnew *systems-dir* asdf:*central-registry*)
(asdf:operate 'asdf:load-op 'cl-swap-file-trunk)