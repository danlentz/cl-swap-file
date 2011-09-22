(in-package :swap-file)

(defun split-to-blocks (data block-size)
  (unless (null data)
    (do ((pos data (cdr pos))
         (count 1 (1+ count)))
        ((>= count block-size)
         (when (null pos)
           (error "null position"))
         (let ((next (cdr pos)))
           (setf (cdr pos) nil)
           (cons data (split-to-blocks next block-size)))))))
    
(defun convert-to-list (data)
  (map 'list #'identity data))

(let ((column 0)
      (offset 0))

  (defun reset-column ()
    (setq column 0))

  (defun format-new-line ()
    (fresh-line)
    (format t "  ")
    (setq column 0))
    
  (defun format-comment (comment)
    (dotimes (i (- 4 column))
      (format t "     "))
    (format t " ; [~4,'0x] ~a" offset comment)
    (format-new-line))
  
  (defun format-octet (v)
    (incf column)
    (incf offset)
    (format t "#x~2,'0x " v)))

(defun format-swap-file-header (block-data &optional (block-size (length block-data)))
  (let ((count 1))
    (format-new-line)
    (map 'nil
         #'(lambda (v)
             (format-octet v)
             (cond 
               ((= count 4)
                (format-comment "SWAP"))
               ((= count 8)
                (format-comment "swap-file version"))
               ((= count 12)
                (format-comment "blocksize"))
               ((= count 16)
                (format-comment "next new offset"))
               ((= count 20)
                (format-comment "available list header"))
               ((and (< 20 count block-size) (= (mod count 4) 0))
                (format-comment "header pad")))
             (incf count))
         block-data)
    (format-comment "header pad")))


(defun format-swap-file-block-header (data &optional (count 0) &aux (v (car data)))
  (incf count)
  (format-octet v)
  (cond 
    ((= count 1)
     (format-comment "block deleted-p"))
    ((= count 5)
     (format-comment "next block offset"))
    ((= count 9)
     (format-comment "data size")))
  (unless (>= count 9)
    (format-swap-file-block-header (cdr data) count)))

(defun format-swap-file-data (data &optional (count 0) &aux (v (car data)))
  (if data
      (progn
        (incf count)
        (format-octet v)
        (when (= (mod count 4) 0)
          (format-comment "data"))
        (format-swap-file-data (cdr data) count))
      (terpri)))

(defun format-swap-file-data-block (block-data &optional (data-formatter #'format-swap-file-data))
  ;;(fresh-line)
  ;;(reset-column)
  (format-swap-file-block-header block-data)
  (funcall data-formatter (nthcdr 9 block-data)))

(defun format-data (data block-size &optional (data-formatter #'format-swap-file-data))
  (let ((blocks (split-to-blocks (convert-to-list data) block-size)))
    (format-swap-file-header (car blocks))
    (mapcar #'(lambda (b) (format-swap-file-data-block b data-formatter)) (cdr blocks)))
  (values))

(defun read-swap-file-data (swap-file)
  (let ((data (make-array (file-length (swap-file-stream swap-file)) :element-type 'unsigned-byte)))
    (file-position (swap-file-stream swap-file) 0)
    (read-sequence data (swap-file-stream swap-file))
    (format t "DATA: ~a~%" data)
    data))

(defun format-swap-file (swap-file &optional (data-formatter #'format-swap-file-data))
  (if (binary-file:binary-array-stream-p (swap-file-stream swap-file))
      (format-data (binary-file:binary-array (swap-file-stream swap-file)) (swap-file-block-size swap-file) data-formatter)
      (format-data  (read-swap-file-data swap-file) (swap-file-block-size swap-file) data-formatter)))
                         