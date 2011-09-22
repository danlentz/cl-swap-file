(in-package :common-lisp-user)

(defpackage swap-file-tests
  (:use :cl :lisp-unit))

(in-package :swap-file-tests)

(define-test max-data-size
  ;; one byte for deleted-p, 4 bytes for data size, 4 bytes for next block.
  (assert-eql (- 128 1 4 4) (swap-file::max-data-size 128)))

(define-test make-empty-data
  (let ((data (swap-file::make-empty-data (swap-file::make-swap-file :version 1 :block-size 128 :available-list 0 :stream (binary-file:make-binary-array-io-stream)))))
    (assert-eql (swap-file::max-data-size 128) (array-dimension data 0))))

(define-test set-block-data
  (let ((disk-block (swap-file::make-disk-block)))
    (assert-eq disk-block (swap-file::set-block-data disk-block #(1 2 3 4 5)))
    (assert-equalp #(1 2 3 4 5) (swap-file::disk-block-data disk-block))
    (assert-eql 5 (swap-file::disk-block-data-size disk-block)))
  (let ((disk-block (swap-file::make-disk-block)))
    (assert-eq disk-block (swap-file::set-block-data disk-block (make-array 5 :initial-contents '(1 2 3 4 5) :fill-pointer t :adjustable t)))
    (assert-equalp #(1 2 3 4 5) (swap-file::disk-block-data disk-block))
    (assert-eql 5 (swap-file::disk-block-data-size disk-block))
    (assert-true (array-has-fill-pointer-p (swap-file::disk-block-data disk-block)))
    (assert-true (adjustable-array-p (swap-file::disk-block-data disk-block)))))

(define-test write-swap-file-header
  (let ((swap-file (swap-file::make-swap-file :version 1 :next-new-offset 60 :block-size 20 :available-list 40 :stream (binary-file:make-binary-array-output-stream))))
    (swap-file::write-swap-file-header swap-file)
    (assert-equalp (make-array 20 :element-type 'unsigned-byte :initial-contents
                               (list (char-code #\S)
                                     (char-code #\W)
                                     (char-code #\A)
                                     (char-code #\P)
                                     1 0 0 0
                                     20 0 0 0
                                     60 0 0 0
                                     40 0 0 0))
                   (binary-file:binary-array (swap-file:swap-file-stream swap-file)))))

(define-test read-swap-file-header
  (let ((swap-file-expected (swap-file::make-swap-file :version 1 :block-size 20 :next-new-offset 60 :available-list 120 :stream (binary-file:make-binary-array-output-stream))))
    (swap-file::write-swap-file-header swap-file-expected)
    (let  ((swap-file (swap-file::make-swap-file :version 1 :block-size 0 :available-list 0 :stream (binary-file:make-binary-array-input-stream (binary-file:binary-array (swap-file:swap-file-stream swap-file-expected))))))
      (assert-eq swap-file (swap-file::read-swap-file-header swap-file))
      (assert-eql 1 (swap-file::swap-file-version swap-file))
      (assert-eql 20 (swap-file::swap-file-block-size swap-file))
      (assert-eql 60 (swap-file::swap-file-next-new-offset swap-file))
      (assert-eql 120 (swap-file::swap-file-available-list swap-file)))))

(define-test create-journal
  (let ((swap-file (swap-file::make-swap-file :version 1 :block-size 20 :next-new-offset 20 :available-list 0 :stream (binary-file:make-binary-array-io-stream))))
    (assert-eq swap-file (swap-file::create-journal swap-file))
    (assert-typep 'wal:wal (swap-file::swap-file-journal swap-file))))

(define-test create
  (let ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20)))
    (assert-typep 'swap-file::swap-file swap-file)
    (assert-eql 1 (swap-file::swap-file-version swap-file))
    (assert-eql 20 (swap-file::swap-file-block-size swap-file))
    (assert-eql 20 (swap-file::swap-file-next-new-offset swap-file))
    (assert-eql  0 (swap-file::swap-file-available-list swap-file))
    (assert-typep 'binary-file:binary-array-io-stream (swap-file:swap-file-stream swap-file))
    (assert-equalp (make-array 20 :element-type 'unsigned-byte :initial-contents
                               (list (char-code #\S)
                                     (char-code #\W)
                                     (char-code #\A)
                                     (char-code #\P)
                                     1 0 0 0
                                     20 0 0 0
                                     20 0 0 0
                                     0 0 0 0))
                   (binary-file:binary-array (swap-file:swap-file-stream swap-file)))
    (assert-typep 'wal:wal (swap-file::swap-file-journal swap-file))))

(define-test allocate-new-block
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file::allocate-new-block swap-file)))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-eql 20 (swap-file::disk-block-offset disk-block))
    (assert-eql 11 (swap-file::disk-block-max-data disk-block))
    (assert-false (swap-file::disk-block-deleted-p disk-block))
    (assert-eql 0 (swap-file::disk-block-next disk-block))
    (assert-eql 0 (swap-file::disk-block-data-size disk-block))
    (assert-equalp #() (swap-file::disk-block-data disk-block))
    (assert-eql 40 (swap-file::swap-file-next-new-offset swap-file))
    (assert-eql 40 (fill-pointer (binary-file:binary-array (swap-file:swap-file-stream swap-file))))))

(define-test allocate-new-block-with-data
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file::allocate-new-block swap-file (swap-file::make-data-array swap-file '(1 2 3 4 5)))))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-eql 40 (length (binary-file:binary-array (swap-file:swap-file-stream swap-file))))
    (assert-eql 20 (swap-file::disk-block-offset disk-block))
    (assert-eql 11 (swap-file::disk-block-max-data disk-block))
    (assert-false (swap-file::disk-block-deleted-p disk-block))
    (assert-eql 0 (swap-file::disk-block-next disk-block))
    (assert-eql 5 (swap-file::disk-block-data-size disk-block))
    (assert-equalp #(1 2 3 4 5) (swap-file::disk-block-data disk-block))
    (assert-eql 40 (swap-file::swap-file-next-new-offset swap-file))
    (assert-eql 40 (fill-pointer (binary-file:binary-array (swap-file:swap-file-stream swap-file))))))

(define-test append-to-available
  (let ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
        (disk-block1 nil)
        (disk-block2 nil))
    (setq disk-block1 (swap-file::allocate-new-block swap-file))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-eq disk-block1 (swap-file::append-to-available disk-block1 swap-file))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-true (swap-file::disk-block-deleted-p disk-block1))
    (assert-eql 0 (swap-file::disk-block-next disk-block1))
    (assert-eql (swap-file::disk-block-offset disk-block1) (swap-file::swap-file-available-list swap-file))
    (assert-eql (swap-file::disk-block-offset disk-block1) (swap-file::read-swap-file-first-available swap-file))
    
    (setq disk-block2 (swap-file::allocate-new-block swap-file))
    (assert-eq disk-block2 (swap-file::append-to-available disk-block2 swap-file))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-true (swap-file::disk-block-deleted-p disk-block2))
    (assert-eql (swap-file::disk-block-offset disk-block1) (swap-file::disk-block-next disk-block2))
    (assert-eql (swap-file::disk-block-offset disk-block2) (swap-file::swap-file-available-list swap-file))
    (assert-eql (swap-file::disk-block-offset disk-block2) (swap-file::read-swap-file-first-available swap-file))))

(define-test get-first-available
  (let ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
        (disk-block1 nil)
        (disk-block2 nil))
    (setq disk-block1 (swap-file::allocate-new-block swap-file))
    (swap-file::append-to-available disk-block1 swap-file)
    (wal:commit (swap-file::swap-file-journal swap-file))
    (setq disk-block2 (swap-file::get-first-available swap-file))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-false (swap-file::disk-block-deleted-p disk-block2))
    (assert-eql 0 (swap-file::disk-block-next disk-block2))
    (assert-eql (swap-file::disk-block-offset disk-block2) (swap-file::disk-block-offset disk-block1))
    (assert-eql 0 (swap-file::read-swap-file-first-available swap-file))))

(define-test write-disk-block
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file::allocate-new-block swap-file (swap-file::make-data-array swap-file '(1 2 3 4 5)))))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-eq disk-block (swap-file::write-disk-block disk-block swap-file))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-equalp #(#x53 #x57 #x41 #x50
                     #x01 #x00 #x00 #x00
                     #x14 #x00 #x00 #x00
                     #x28 #x00 #x00 #x00
                     #x00 #x00 #x00 #x00
                     
                     #x00 ;; deleted-p
                     #x00 #x00 #x00 #x00 ;; next
                     #x05 #x00 #x00 #x00 ;; data size
                     #x01 #x02 #x03 #x04 #x05 ;; data
                     #x00 #x00 #x00 #x00 #x00 #x00 ;; not used data
                     ) 
                   (binary-file:binary-array (swap-file:swap-file-stream swap-file)))

    ;; toggle deleted-p
    (setf (swap-file::disk-block-deleted-p disk-block) t)
    (assert-eq disk-block (swap-file::write-disk-block disk-block swap-file))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-equalp #(#x53 #x57 #x41 #x50
                     #x01 #x00 #x00 #x00
                     #x14 #x00 #x00 #x00
                     #x28 #x00 #x00 #x00
                     #x00 #x00 #x00 #x00
                     
                     #x01 ;; deleted-p
                     #x00 #x00 #x00 #x00 ;; next
                     #x05 #x00 #x00 #x00 ;; data size
                     #x01 #x02 #x03 #x04 #x05 ;; data
                     #x00 #x00 #x00 #x00 #x00 #x00 ;; not used data
                     ) 
                   (binary-file:binary-array (swap-file:swap-file-stream swap-file)))
    
    ;; set next
    (setf (swap-file::disk-block-next disk-block) #x7b)
    (assert-eq disk-block (swap-file::write-disk-block disk-block swap-file))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-equalp #(#x53 #x57 #x41 #x50
                     #x01 #x00 #x00 #x00
                     #x14 #x00 #x00 #x00
                     #x28 #x00 #x00 #x00
                     #x00 #x00 #x00 #x00
                     
                     #x01 ;; deleted-p
                     #x7b #x00 #x00 #x00 ;; next
                     #x05 #x00 #x00 #x00 ;; data size
                     #x01 #x02 #x03 #x04 #x05 ;; data
                     #x00 #x00 #x00 #x00 #x00 #x00 ;; not used data
                     ) 
                   (binary-file:binary-array (swap-file:swap-file-stream swap-file)))))

(define-test next-p
  (assert-false (swap-file::next-p (swap-file::make-disk-block :next 0)))
  (assert-true   (swap-file::next-p (swap-file::make-disk-block :next 1234))))


(define-test split-data
  (let ((chunks (swap-file::split-data nil 5)))
    (assert-false chunks))

  (let ((chunks (swap-file::split-data #() 5)))
    (assert-eql 1 (length chunks))
    (assert-equalp #() (first chunks)))

  (let ((chunks (swap-file::split-data (make-array 5 :initial-contents '(1 2 3 4 5) :fill-pointer t :adjustable t) 5)))
    (assert-eql 1 (length chunks))
    (assert-equalp #(1 2 3 4 5) (first chunks)))

  (let ((chunks (swap-file::split-data #(1 2 3 4 5) 1)))
    (assert-eql 5 (length chunks))
    (assert-equalp #(1) (first chunks))
    (assert-equalp #(2) (second chunks))
    (assert-equalp #(3) (third chunks))
    (assert-equalp #(4) (fourth chunks))
    (assert-equalp #(5) (fifth chunks)))

  (let ((chunks (swap-file::split-data #(1 2 3 4 5) 2)))
    (assert-eql 3 (length chunks))
    (assert-equalp #(1 2) (first chunks))
    (assert-equalp #(3 4) (second chunks))
    (assert-equalp #(5) (third chunks)))
  
  (let ((chunks (swap-file::split-data (make-array 20 :adjustable t :fill-pointer t :initial-contents '(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20)) (swap-file::max-data-size 20))))
    (assert-eql 2 (length chunks))
    (assert-equalp #(1 2 3 4 5 6 7 8 9 10 11) (first chunks))
    (assert-equalp #(12 13 14 15 16 17 18 19 20) (second chunks))))

(define-test merge-data
  (let ((result (swap-file::merge-data '(#(1) #(2) #(3)))))
    (assert-equalp #(1 2 3) result)
    (assert-true (array-has-fill-pointer-p result))
    (assert-true (adjustable-array-p result)))

  (let ((result (swap-file::merge-data '(#(1 2 3) #(4 5 ) #(6 7 8 9)))))
    (assert-equalp #(1 2 3 4 5 6 7 8 9) result)
    (assert-true (array-has-fill-pointer-p result))
    (assert-true (adjustable-array-p result)))

  (let ((result (swap-file::merge-data '(#(1 2 3)))))
    (assert-equalp #(1 2 3) result)
    (assert-true (array-has-fill-pointer-p result))
    (assert-true (adjustable-array-p result)))

  (assert-false (swap-file::merge-data nil)))

(define-test offset
  (assert-eql 20 (swap-file::offset (swap-file::make-disk-block :offset 20)))
  (assert-eql  0 (swap-file::offset nil)))

(define-test write-connected-blocks
  (let ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
        (disk-block nil))
    (setq disk-block (swap-file::create-block swap-file))
    (swap-file::write-connected-blocks nil swap-file (swap-file::disk-block-offset disk-block))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-equalp #(#x53 #x57 #x41 #x50
                     #x01 #x00 #x00 #x00
                     #x14 #x00 #x00 #x00
                     #x28 #x00 #x00 #x00
                     #x14 #x00 #x00 #x00
                       
                     #x00 ;; deleted-p
                     #x00 #x00 #x00 #x00 ;; next
                     #x00 #x00 #x00 #x00 ;; data size
                     #x00 #x00 #x00 #x00
                     #x00 #x00 #x00 #x00
                     #x00 #x00 #x00 ;; data
                     )
                   (binary-file:binary-array (swap-file:swap-file-stream swap-file))))

  (let ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
        (disk-block nil))
    (setq disk-block (swap-file::create-block swap-file))
    (swap-file::write-connected-blocks '(#()) swap-file (swap-file::disk-block-offset disk-block))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-equalp #(#x53 #x57 #x41 #x50
                     #x01 #x00 #x00 #x00
                     #x14 #x00 #x00 #x00
                     #x28 #x00 #x00 #x00
                     #x00 #x00 #x00 #x00
                       
                     #x00 ;; deleted-p
                     #x00 #x00 #x00 #x00 ;; next
                     #x00 #x00 #x00 #x00 ;; data size
                     #x00 #x00 #x00 #x00
                     #x00 #x00 #x00 #x00
                     #x00 #x00 #x00 ;; data
                     )
                   (binary-file:binary-array (swap-file:swap-file-stream swap-file))))

  (let ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
        (disk-block nil))
    (setq disk-block (swap-file:create-block swap-file))
    (swap-file::write-connected-blocks (swap-file::split-data (make-array 20 :adjustable t :fill-pointer t :initial-contents '(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20)) (swap-file::max-data-size 20)) swap-file (swap-file::disk-block-offset disk-block))
    (wal:commit (swap-file::swap-file-journal swap-file))
    (assert-equalp #(#x53 #x57 #x41 #x50
                     #x01 #x00 #x00 #x00
                     #x14 #x00 #x00 #x00
                     #x3c #x00 #x00 #x00
                     #x00 #x00 #x00 #x00
                       
                     #x00 ;; deleted-p
                     #x28 #x00 #x00 #x00 ;; next
                     #x0b #x00 #x00 #x00 ;; data size
                     #x01 #x02 #x03 #x04 #x05 #x06 #x07 #x08 #x09 #x0a #x0b ;; data
                     ;; exceeding block
                     #x00 ;; deleted-p
                     #x00 #x00 #x00 #x00 ;; next
                     #x09 #x00 #x00 #x00 ;; data size
                     #x0c #x0d #x0e #x0f #x10 #x11 #x12 #x13 #x14  ;; exceeding data
                     #x00 #x00 ;; not in use
                     ) 
                   (binary-file:binary-array (swap-file:swap-file-stream swap-file)))))
    
(define-test write-data
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file:create-block swap-file)))
    (swap-file::write-data #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20) swap-file (swap-file::disk-block-offset disk-block))
    (swap-file:flush swap-file)
    (assert-equalp #(#x53 #x57 #x41 #x50
                     #x01 #x00 #x00 #x00
                     #x14 #x00 #x00 #x00
                     #x3c #x00 #x00 #x00
                     #x00 #x00 #x00 #x00
                       
                     #x00 ;; deleted-p
                     #x28 #x00 #x00 #x00 ;; next
                     #x0b #x00 #x00 #x00 ;; data size
                     #x01 #x02 #x03 #x04
                     #x05 #x06 #x07 #x08
                     #x09 #x0a #x0b ;; data

                     #x00 ;; deleted-p
                     #x00 #x00 #x00 #x00 ;; next
                     #x09 #x00 #x00 #x00 ;; data size
                     #x0c #x0d #x0e #x0f
                     #x10 #x11 #x12 #x13
                     #x14 #x00 #x00 ;; data
                     )
                   (binary-file:binary-array (swap-file:swap-file-stream swap-file)))))
    
(define-test read-disk-block
  (let ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
        (expected-disk-block nil))
    (setq expected-disk-block (swap-file::write-disk-block (swap-file::allocate-new-block (swap-file::write-swap-file-header swap-file)
                                                                    (swap-file::make-data-array swap-file '(1 2 3 4 5)))
                                                swap-file))
    (swap-file:flush swap-file)
    (let ((disk-block (swap-file::read-disk-block swap-file (swap-file::disk-block-offset expected-disk-block))))
      (assert-eql (swap-file::disk-block-offset expected-disk-block)
                  (swap-file::disk-block-offset disk-block))
      (assert-false (swap-file::disk-block-deleted-p disk-block))
      (assert-eql 0 (swap-file::disk-block-next disk-block))
      (assert-eql 5 (swap-file::disk-block-data-size disk-block))
      (assert-equalp (swap-file::disk-block-data expected-disk-block) (swap-file::disk-block-data disk-block)))))

(define-test read-connected-blocks
  (let ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
        (expected-disk-block nil))
    (setq expected-disk-block (swap-file:create-block swap-file))
    (swap-file::write-connected-blocks (swap-file::split-data (make-array 20 :adjustable t :fill-pointer t :initial-contents '(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20)) (swap-file::max-data-size 20)) swap-file (swap-file::disk-block-offset expected-disk-block))
    (swap-file:flush swap-file)
    (let ((disk-blocks (swap-file::read-connected-blocks swap-file (swap-file::disk-block-offset expected-disk-block))))
      (assert-typep 'cons disk-blocks)
      (assert-eql 2 (length disk-blocks))
      (assert-equalp (swap-file::read-disk-block swap-file (swap-file::disk-block-offset expected-disk-block)) (first disk-blocks))
      (assert-equalp (swap-file::read-disk-block swap-file (swap-file::disk-block-next expected-disk-block)) (second disk-blocks)))))

(define-test read-data
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file:create-block swap-file)))
    (swap-file::write-data #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20) swap-file (swap-file::disk-block-offset disk-block))
    (swap-file:flush swap-file)
    (assert-equalp #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20)
                   (swap-file::read-data swap-file (swap-file::disk-block-offset disk-block)))))

(define-test create-block-stream
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (s (swap-file:create-block-stream swap-file))
         (offset (swap-file::stream-offset s)))
    (print "hello world! how is it going?" s)
;;    (format t "~a~%" (disk-block-data (stream-disk-block s)))
;;    (format t "~a~%" (binary-file:binary-array s))
    (finish-output s)
    (let ((arr (binary-file:binary-array (swap-file:swap-file-stream swap-file))))
      (declare (ignore arr))
      (cl:close s)
      (setq s (swap-file:open-block-stream swap-file offset))
      (assert-equal "hello world! how is it going?" (read s)))))

(define-test with-open-block-stream
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file:create-block swap-file)))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (print "hello again! just testing if this works at all." s))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (assert-equal "hello again! just testing if this works at all." (read s)))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (print "wonder what happens now?" s))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (assert-equal "wonder what happens now?" (read s)))))

(define-test write-multiple-objects
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file:create-block swap-file)))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (print "foo" s)
      (print "bar" s)
      (print 1234 s)
      (print 'test-symbol s))
   (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
     (assert-equal "foo" (read s))
     (assert-equal "bar" (read s))
     (assert-equal 1234 (read s))
     (assert-equal 'test-symbol (read s))
     (assert-false (read s nil nil)))))

(define-test write-sequence
  (let* ((swap-file (swap-file:create  (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file:create-block swap-file)))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (write-sequence #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20) s))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (let ((seq (make-array 20 :element-type 'integer :initial-element 0)))
        (read-sequence seq s)
        (assert-equalp #(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20)  seq)))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (let ((seq (make-array 20 :element-type 'integer :initial-element 0)))
        (read-sequence seq s :start 10 :end 15)
        (assert-equalp #(0 0 0 0 0 0 0 0 0 0 1 2 3 4 5 0 0 0 0 0) seq))))
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file:create-block swap-file)))
    ;; bug here! the contents of disk-block data is not '(unsigned-byte 8) after write-sequence.
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (write-sequence (make-array 4 :element-type '(unsigned-byte 32) :initial-contents '(#xffffffff #xfffffffe #xfffffffd #xfffffffc)) s))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
;;      (format t "~a~%" (disk-block-data (stream-disk-block s)))
      (let ((seq (make-array 4 :element-type '(unsigned-byte 32) :initial-element 0)))
        (read-sequence seq s)
        (assert-equalp #(#xffffffff #xfffffffe #xfffffffd #xfffffffc) seq)))))

(define-test file-position
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file:create-block swap-file)))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (assert-eql 0 (file-position s))
      (file-position s 5)
      (assert-eql 5 (file-position s))
      (princ 'HELLO s) ; using print causes stream to get messed up. Happens with normal file also.
      (file-position s 5)
      (assert-equal 'HELLO (read s nil nil))
      (file-position s 0)
      (assert-equal (map 'string #'code-char '(0 0 0 0 0 72 69 76 76 79)) (symbol-name (read s nil nil))))))

(define-test file-position-beyond-block-size
  (let* ((swap-file (swap-file:create (binary-file:make-binary-array-io-stream) :block-size 20))
         (disk-block (swap-file:create-block swap-file)))
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (assert-eql 0 (file-position s))
      (file-position s 50)
      (assert-eql 50 (file-position s))
      (princ 'HELLO s) ; using print causes stream to get messed up. Happens with normal file also.
      (file-position s 50)
      (assert-equal 'HELLO (read s nil nil))
      (file-position s 45)
      (assert-equal (map 'string #'code-char '(0 0 0 0 0 72 69 76 76 79)) (symbol-name (read s nil nil))))
    (swap-file:flush swap-file)
    (swap-file:with-open-block-stream (s swap-file (swap-file:disk-block-offset disk-block))
      (file-position s 50)
      (assert-equal 'HELLO (read s nil nil)))))


