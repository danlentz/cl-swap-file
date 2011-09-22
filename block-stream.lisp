(in-package :swap-file)

(defclass disk-block-stream (binary-file:binary-array-io-stream)
  ((option :initarg :option :reader stream-option)
   (offset :initarg :offset :reader stream-offset)
   (modified :initform nil :accessor stream-modified)
   (swap-file  :initarg :swap-file :reader stream-swap-file))
  (:default-initargs :offset 0 :swap-file nil :option nil))

(defmethod trivial-gray-streams:stream-clear-output ((stream disk-block-stream))
  (declare (ignore stream))
  nil)

(defmethod trivial-gray-streams:stream-finish-output ((stream disk-block-stream))
  (declare (ignore stream))
  nil)

(defmethod trivial-gray-streams:stream-force-output ((stream disk-block-stream))
  (declare (ignore stream))
  nil)

(defmethod trivial-gray-streams:stream-write-byte :after ((stream disk-block-stream) integer)
  (setf (stream-modified stream) t))

(defmethod common-lisp:close ((stream disk-block-stream) &key abort)
  (when (stream-modified stream)
    (unless abort
      (write-data (binary-file:binary-array stream) (stream-swap-file stream) (stream-offset stream)))))

(defun make-disk-block-stream (offset swap-file &key option)
  (make-instance 'disk-block-stream :offset offset :swap-file swap-file :option option))

(defmethod initialize-instance :after ((stream disk-block-stream) &key)
  (if (eql (stream-option stream) :truncate)
      (setf (slot-value stream 'binary-file:binary-array) (make-empty-data (stream-swap-file stream)))
      (setf (slot-value stream 'binary-file:binary-array) (read-data (stream-swap-file stream) (stream-offset stream))))
  (when (eql (stream-option stream) :append)
    (file-position stream :end)))

(defun create-block-stream (swap-file)
  (make-disk-block-stream (disk-block-offset (create-block swap-file)) swap-file))

(defun open-block-stream (swap-file offset &key option)
  (make-disk-block-stream offset swap-file :option option))

(defmacro with-open-block-stream ((stream swap-file offset &key option) &body body)
  (let ((sf (gensym)))
    `(let* ((,sf ,swap-file)
            (,stream (open-block-stream ,sf ,offset :option ,option)))
       (unwind-protect
            (progn
              ,@body)
         (cl:close ,stream)))))
