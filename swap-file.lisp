(in-package :swap-file)

(declaim (optimize (space 0) (speed 0) (debug 3) (safety 2))) ; development mode

(defstruct swap-file
  (version           1 :type (unsigned-byte 32))
  (block-size     4096 :type (unsigned-byte 32))
  (next-new-offset   0 :type (unsigned-byte 32))
  (available-list    0 :type (unsigned-byte 32)) ; offset to first available block or 0 if none (= need to create one)
  (block-cache     nil :type list)               ; list of weak pointers pointing to a disk blocks.
  (open-blocks     nil :type list)               ; list of currently open disk blocks. This is cleared after flush.
  (journal         nil :type (or wal:wal null))
  (stream          nil :type stream))

(defstruct disk-block
  (offset    0   :type (unsigned-byte 32)) ; file position
  (max-data  0   :type (unsigned-byte 32)) ; maximum length of data to fit in this block
  (deleted-p nil :type t)                  ; 1 byte on disk, t = 0, nil = 1
  (next      0   :type (unsigned-byte 32)) ; 4 bytes
  (data-size 0   :type (unsigned-byte 32)) ; 4 bytes
  (data      nil :type (or array null)))

(defun available-p (swap-file)
  "Returns t if there is available (deleted) blocks on swap-file, nil otherwise."
  (/= (swap-file-available-list swap-file) 0))

(defun max-data-size (block-size)
  "Returns maximum size of payload data. This size block-size reduced by the size of block header."
  (- block-size 1 4 4)) ;; deleted-p = 1 byte, next = 4 bytes, data-size = 4 bytes

(defun make-empty-data (swap-file)
  "Returns an empty array of bytes. Array's initial size is max-data-size but the array is adjustable."
  (make-array (max-data-size (swap-file-block-size swap-file)) :adjustable t :fill-pointer 0 :element-type 'unsigned-byte :initial-element 0))

(defun make-data-array (swap-file initial-contents)
  "Returns an array of bytes with given initial contents."
  (map-into (make-array (max (length initial-contents) (max-data-size (swap-file-block-size swap-file))) :adjustable t :fill-pointer t :element-type 'unsigned-byte :initial-element 0) #'identity initial-contents))


(defun wal-entry-writer (entry stream)
  "Writes a journal entry to a stream. The function is writer for write-ahead log."
  ;; write master offset
  (little-endian:write-uint32 (car entry) stream)
  ;; write sequence size
  (little-endian:write-uint32 (array-dimension (cdr entry) 0) stream)
  ;; write sequence
  (write-sequence (cdr entry) stream))

(defun wal-entry-reader (stream)
  "Reads a journal entry from stream. The function is reader for write-ahead log."
  ;; read master offset
  (cons (little-endian:read-uint32 stream)
        (read-sequence (make-array (little-endian:read-uint32 stream)
                                   :element-type 'unsigned-byte)
                       stream)))

(defun make-master-writer (swap-file)
  "Returns master writer function. The master writer is called by
write-ahead log when an entry is committed to swap-file's stream.
Master writer locates entry's file position and writes entry's
contents to that position."
  (let ((swap-file swap-file))
    (lambda (entry)
      (file-position (swap-file-stream swap-file) (car entry))
      (write-sequence (cdr entry) (swap-file-stream swap-file)))))
;;      (finish-output (swap-file-stream swap-file)))))
;;      (write-disk-block disk-block swap-file))))

(defun write-sequence-to-disk (data swap-file)
  "Write sequence to disk. Sequence is written to a write ahead log."
  (wal:write (swap-file-journal swap-file) (cons (file-position (swap-file-stream swap-file))
                                             data)))
  ;;  (write-sequence data (swap-file-stream swap-file)))

(defun write-uint32-to-disk (value swap-file)
  "Write 32-bit unsigned byte to disk. Value is written to a write ahead log."
  (write-sequence-to-disk 
   (binary-file:with-output-to-binary-array (out)
     (little-endian:write-uint32 value out))
   swap-file))
    
(defun write-uint8-to-disk (value swap-file)
  "Write 8-bit unsigned byte to disk. Value is written to a write ahead log."
  (write-sequence-to-disk 
   (little-endian:write-uint8 value (swap-file-stream swap-file))
   swap-file))

(defun write-swap-file-header (swap-file)
  "Write swap-file's header to a swap-file's stream."
  (write-sequence
   (binary-file:with-output-to-binary-array (s)
     (little-endian:write-uint8 (char-code #\S) s)
     (little-endian:write-uint8 (char-code #\W) s)
     (little-endian:write-uint8 (char-code #\A) s)
     (little-endian:write-uint8 (char-code #\P) s)
     (little-endian:write-uint32 (swap-file-version swap-file)    s)
     (little-endian:write-uint32 (swap-file-block-size swap-file) s)
     (little-endian:write-uint32 (swap-file-next-new-offset swap-file) s)
     (little-endian:write-uint32 (swap-file-available-list swap-file) s)
     ;; pad 0's till end of the first block. 
     (write-sequence (make-array (- (swap-file-block-size swap-file) (swap-file-header-size)) :initial-element 0 :element-type 'unsigned-byte) s))
   (swap-file-stream swap-file))
  (finish-output (swap-file-stream swap-file))
  swap-file)

(defun read-swap-file-next-new-offset (swap-file)
  "Reads and returns offset of the next new disk block."
  (file-position (swap-file-stream swap-file) 12)
  (little-endian:read-uint32 (swap-file-stream swap-file)))

(defun write-swap-file-next-new-offset (swap-file)
  "Writes offset of the next new disk block to swap-file's stream."
  (file-position (swap-file-stream swap-file) 12)
  (write-uint32-to-disk (swap-file-next-new-offset swap-file) swap-file))

(defun read-swap-file-first-available (swap-file)
  "Reads and returns offset of first available (deleted) disk block. If
there is no available blocks, returns 0."
  (file-position (swap-file-stream swap-file) 16)
  (little-endian:read-uint32 (swap-file-stream swap-file)))

(defun write-swap-file-first-available (offset swap-file)
  "Writes offset of first available (deleted) disk block to the
swap-file's stream."
  (file-position (swap-file-stream swap-file) 16)
  (write-uint32-to-disk offset swap-file))

(defun read-swap-file-header (swap-file)
  "Reads swap-file header and updates swap-file structure. Returns updated swap-file structure."
  (unless (and (= (little-endian:read-uint8 (swap-file-stream swap-file)) (char-code #\S))
               (= (little-endian:read-uint8 (swap-file-stream swap-file)) (char-code #\W))
               (= (little-endian:read-uint8 (swap-file-stream swap-file)) (char-code #\A))
               (= (little-endian:read-uint8 (swap-file-stream swap-file)) (char-code #\P)))
    (error "Not a swap file."))
  (unless (= (swap-file-version swap-file) (little-endian:read-uint32 (swap-file-stream swap-file)))
    (error "Swap file version does not match."))
  (setf (swap-file-block-size swap-file) (little-endian:read-uint32 (swap-file-stream swap-file))
        (swap-file-next-new-offset swap-file) (little-endian:read-uint32 (swap-file-stream swap-file))
        (swap-file-available-list swap-file) (little-endian:read-uint32 (swap-file-stream swap-file)))
  swap-file)

(defun swap-file-header-size ()
  "Returns size of swap-file header."
  (+ 4 ; "SWAP"
     4 ; version
     4 ; block size
     4 ; next new offset
     4 ; next available offset
     ))

(defun set-block-data (disk-block data)
  "Sets disk-blocks data. Returns updated disk-block."
  (setf (disk-block-data disk-block) data)
  (if (array-has-fill-pointer-p data)
      (setf (disk-block-data-size disk-block) (fill-pointer data))
      (setf (disk-block-data-size disk-block) (length data)))
  disk-block)

(defun allocate-new-block (swap-file &optional (data (make-empty-data swap-file)))
  "Returns new empty block from swap file. Disk block is allocated
from end of the file if none is available.  Empty block data array
contains only zeros and fill pointer is set to 0. If data is given,
the given data is to allocated block but not flushed to the disk. In
that case caller should call write-disk-block after allocation."
  (declare (optimize (speed 0) (space 0) (debug 3)))
  (let ((new-block (make-disk-block :offset (swap-file-next-new-offset swap-file)
                                    ;;(file-position (swap-file-stream swap-file))
                                    :max-data (max-data-size (swap-file-block-size swap-file))
                                    :deleted-p nil
                                    :next 0
                                    :data-size (fill-pointer data)
                                    :data data)))

    ;; write all 0's to new block.
    (file-position (swap-file-stream swap-file) (swap-file-next-new-offset swap-file))
    (write-sequence-to-disk (make-array (swap-file-block-size swap-file) :element-type 'unsigned-byte :initial-element 0)
                            swap-file)

    ;; increment next new offset
    (incf (swap-file-next-new-offset swap-file) (swap-file-block-size swap-file))
    (write-swap-file-next-new-offset swap-file)

    new-block))

(defun append-to-available (disk-block swap-file)
  "Pushes given disk-block to swap-file's available list. Available
list is updated at disk."
  ;; link block to available list.
  (setf (disk-block-next disk-block) (swap-file-available-list swap-file))
  (setf (swap-file-available-list swap-file) (disk-block-offset disk-block))
  (write-swap-file-first-available (disk-block-offset disk-block) swap-file)

  ;; seek to disk block
  (file-position (swap-file-stream swap-file) (disk-block-offset disk-block))
  
  ;; mark block as deleted
  (setf (disk-block-deleted-p disk-block) t)
  (write-uint8-to-disk 1 swap-file)

  ;; write next available to disk
  (write-uint32-to-disk (disk-block-next disk-block) swap-file)
  disk-block)

(defun get-first-available (swap-file)
  "Returns first available (deleted) disk block or nil if there is no available disk blocks."
  (when (available-p swap-file)
    (let ((disk-block (read-disk-block swap-file (swap-file-available-list swap-file))))
      (setf (swap-file-available-list swap-file) (disk-block-next disk-block))
      (write-swap-file-first-available (disk-block-next disk-block) swap-file)
      
      ;; seek to disk block
      (file-position (swap-file-stream swap-file) (disk-block-offset disk-block))
    
      ;; mark block as undeleted
      (setf (disk-block-deleted-p disk-block) nil)
      (write-uint8-to-disk 0 swap-file)
      
      ;; write next available to disk
      (setf (disk-block-next disk-block) 0)
      (write-uint32-to-disk 0 swap-file)
      disk-block)))

(defun read-disk-block-array (stream block-size)
  "Reads and returns (single) disk block's data as an array of bytes."
  (let ((array (make-array block-size :element-type 'unsigned-byte :initial-element 0))
        (result 0))
    (setq result (read-sequence array stream))
    (assert (= block-size result))
    array))

(defun read-disk-block (swap-file offset)
  "Reads and returns disk block from swap file. If disk node is marked as deleted, the
data is not read."
  (file-position (swap-file-stream swap-file) offset)
  (binary-file:with-input-from-binary-array (s (read-disk-block-array (swap-file-stream swap-file)
                                                                      (swap-file-block-size swap-file)))
    (let ((disk-block (make-disk-block :offset offset
                                       :max-data (max-data-size (swap-file-block-size swap-file))
                                       :deleted-p (= (read-byte s) 1)
                                       :next (little-endian:read-uint32 s)
                                       :data-size (little-endian:read-uint32 s)
                                       :data (make-empty-data swap-file))))
      (unless (disk-block-deleted-p disk-block)
        (setf (fill-pointer (disk-block-data disk-block)) (disk-block-data-size disk-block))
        (read-sequence (disk-block-data disk-block) s :start 0))
      disk-block)))

(defun read-connected-blocks (swap-file offset)
  "Returns a list of connected disk blocks read from swap-file starting from disk block at given offset."
  (let ((disk-block (open-block swap-file offset)))
    (if (= (disk-block-next disk-block) 0)
      (list disk-block)
      (cons disk-block (read-connected-blocks swap-file (disk-block-next disk-block))))))

(defun merge-data (data-chunks)
  "Returns a new adjustable array with fill pointer. Arrays size is
equal to sum of given data chunks and contents are merged from data
chunks. Data chunks are arrays themselves."
  (when data-chunks
    (let ((new-data (make-array (reduce #'+ data-chunks :key #'data-size :initial-value 0)
                                :initial-element 0 :element-type 'unsigned-byte :adjustable t :fill-pointer t))
          (start 0))
      (mapcar #'(lambda (data-chunk)
                  (setf (subseq new-data start (+ start (data-size data-chunk))) data-chunk)
                  (incf start (data-size data-chunk)))
              data-chunks)
      new-data)))

(defun data-size-below-max (disk-block)
  "Returns disk blocks data size or disk block maximum data size if
current disk block's data size is over than maximum."
  (min (disk-block-data-size disk-block)
       (disk-block-max-data disk-block)))

(defgeneric data-size (obj)
  (:method ((obj disk-block))
    (disk-block-data-size obj))
  (:method ((obj array))
    (if (array-has-fill-pointer-p obj)
        (fill-pointer obj)
        (length obj))))

(defun next-p (disk-block)
  "Returns t if disk bock has next block set, nil otherwise."
  (/= (disk-block-next disk-block) 0))

(defun split-data (data max-size)
  "Returns data splitted to a list of byte arrays with each having size below max-size."
  (when data
    (if (= (length data) 0)
        '(#())
        (do ((pos 0 (+ pos max-size))
             (chunks nil))
            ((>= pos (length data)) (reverse chunks))
          (push (make-array (min (- (length data) pos) max-size) :displaced-to data :displaced-index-offset pos) chunks)))))
       
(defun offset (disk-block)
  "Returns disk block's offset or 0 if disk block is nil."
  (if disk-block
      (disk-block-offset disk-block)
      0))

(defun write-connected-blocks (data-chunks swap-file offset)
  "Writes data splitted to data-chunks to the swap-file starting from
disk block at given offset. The old disk block contents are
overwritten. If the data is shorter than old data the exceeding disk
blocks are deleted. If the data is longer than old data new disk
blocks are allocated as needed."
  (cond 
    ((and (null data-chunks) (= offset 0))
     nil)
    ;; unlink old connected blocks if there is no more data left.
    ((and (null data-chunks) (> offset 0))
     (unlink-block swap-file offset)
     nil)
    ;; allocate new block.
    ((and data-chunks (= offset 0))
       (write-disk-block (create-block swap-file :next (offset (write-connected-blocks (rest data-chunks) swap-file 0)) :data (first data-chunks)) swap-file))
    ;; write over old connected block.
    (t ; (and data-chunks (> offset 0))
     (let ((disk-block (open-block swap-file offset)))
       (setf (disk-block-next disk-block) (offset (write-connected-blocks (rest data-chunks) swap-file (disk-block-next disk-block))))
       (write-disk-block (set-block-data (open-block swap-file offset) (first data-chunks)) swap-file)))))

;; (defun delete-extending (disk-block swap-file)
;;   "Unlinks extending blocks from given disk block, if such exist."
;;   (when (next-p disk-block)
;;     (unlink-block swap-file (open-block swap-file (disk-block-next disk-block)))
;;     (setf (disk-block-next disk-block) 0)))

(defun write-disk-block (disk-block swap-file)
  "Write disk block to swap file. The disk block must be allocated
before it can be written.  If the data array contents exceed block
size new blocks are allocated until all data can fit and also
exceeding data is written to the disk."

  ;; if sequence is over max data, write extending data to existing or new extending block.
  ;; this called first because we want to get next block's offset before writing
  ;; current block on disk.
;;   (if (> (fill-pointer (disk-block-data disk-block)) (disk-block-max-data disk-block))
;;       (write-extending disk-block swap-file)
;;       (delete-extending disk-block swap-file))

  ;; locate disk-block on swap-file
  (file-position (swap-file-stream swap-file) (disk-block-offset disk-block))

  ;; write disk block to disk.
  (write-sequence-to-disk
   ;; construct full disk block array using in memory stream.
   (binary-file:with-output-to-binary-array (out)
     (let ((allowed-size (data-size-below-max disk-block)))
       ;; write deleted-p
       (write-byte (if (disk-block-deleted-p disk-block) 1 0) out)
       ;; write next block offset
       (little-endian:write-uint32 (disk-block-next disk-block) out)
       ;; write data-size
       (little-endian:write-uint32 allowed-size out)
       ;; write data
       (write-sequence (disk-block-data disk-block) out :end allowed-size)))
   swap-file)
  disk-block)

;;;
;;; exported functions
;;;

(defun flush (swap-file)
  "Forces all written changes to be written on to swap-file's file."
  (if (swap-file-journal swap-file)
      (wal:commit (swap-file-journal swap-file)))
  (finish-output (swap-file-stream swap-file))
  (setf (swap-file-open-blocks swap-file) nil)
  swap-file)

(defun rollback (swap-file)
  "Discard all changes to a swap-file."
  (wal:rollback (swap-file-journal swap-file))
  swap-file)

(defun create-journal (swap-file)
  "Create journal for a swap-file."
  (setf (swap-file-journal swap-file) (wal:open (swap-file-stream swap-file) (make-master-writer swap-file) :if-exists :error :if-does-not-exist :create :entry-writer #'wal-entry-writer :entry-reader #'wal-entry-reader))
  swap-file)

(defun open-journal (swap-file)
  "Open existing journal for a swap-file."
  (setf (swap-file-journal swap-file) (wal:open (swap-file-stream swap-file) (make-master-writer swap-file) :if-exists :overwrite :if-does-not-exist :error :entry-writer #'wal-entry-writer :entry-reader #'wal-entry-reader))
  (wal:recover (swap-file-journal swap-file))
  swap-file)

(defun create (filespec &key (if-exists :error) (block-size 4096))
  "Creates and returns new swap-file. Swap-file is created to given filespec."
  (assert (>= block-size 20))
  (write-swap-file-header
   (create-journal (make-swap-file :version 1 :block-size block-size :next-new-offset block-size :stream (binary-file:open-binary-stream filespec :if-exists if-exists :if-does-not-exist :create)))))

(defun open (filespec &key (if-exists :overwrite) (if-does-not-exist :error))
  "Opens and returns existing swap-file."
  (open-journal
   (read-swap-file-header (make-swap-file :version 1 :stream (binary-file:open-binary-stream filespec :if-exists if-exists :if-does-not-exist if-does-not-exist)))))

(defun close (swap-file)
  "Closes swap-file."
  (flush swap-file)
  (cl:close (swap-file-stream swap-file)))

(defun push-to-cache (swap-file disk-block)
  "Push disk block to swap-file disk block cache."
  (push (tg:make-weak-pointer disk-block) (swap-file-block-cache swap-file))
  disk-block)

(defun get-from-cache (swap-file offset)
  "Returns disk block from cache, or nil if disk block with given
offset does not exist."
  (do* ((pos (swap-file-block-cache swap-file) (cdr pos))
        (db nil))
       ((null pos) db)
    (let ((v (tg:weak-pointer-value (car pos))))
      (when (and v (= (disk-block-offset v) offset))
        (setq db v)))))

(defun create-block (swap-file &key (next 0) (data (make-empty-data swap-file)))
  "Allocates and returns empty disk block from swap-file."
  (let ((disk-block
         (if (available-p swap-file)
             (push-to-cache swap-file (get-first-available swap-file))
             (push-to-cache swap-file (allocate-new-block swap-file)))))
    (setf (disk-block-next disk-block) next)
    (push disk-block (swap-file-open-blocks swap-file))
    (set-block-data disk-block data)))

(defun open-block (swap-file offset)
  "Opens and returns existing disk block from swap-file."
  (let ((disk-block (get-from-cache swap-file offset)))
    (unless disk-block
        (setq disk-block (push-to-cache swap-file (read-disk-block swap-file offset))))
    (push disk-block (swap-file-open-blocks swap-file))
    disk-block))

(defun read-data (swap-file offset)
  "Returns an adjustable array with fill pointer. Function read-data
reads connected disk blocks from swap-file starting from given
offset. If disk block extends to next block, the data of consequtive
blocks are automatically appended to the data array of the read
block. In other words, returned data array may contain data from
multiple disk blocks."
  (merge-data (mapcar #'disk-block-data (read-connected-blocks swap-file offset))))

(defun write-data (data swap-file offset)
  "Write data to a swap-file and to a given offset."
  (write-connected-blocks (split-data data (max-data-size (swap-file-block-size swap-file)))
                          swap-file offset))


(defun close-block (swap-file disk-block)
  "Closes disk block."
  (write-disk-block disk-block swap-file))
 ;; (wal:write (swap-file-journal swap-file) disk-block))

(defun ensure-disk-block (disk-block-spec swap-file)
  "Returns or opens and returns a disk block."
  (etypecase disk-block-spec
    (disk-block disk-block-spec)
    (integer (open-block swap-file disk-block-spec))))
    
(defun unlink-block (swap-file disk-block-spec &aux (disk-block (ensure-disk-block disk-block-spec swap-file)))
  "Unlinks disk block. Disk block is pushed into available disk blocks list."
  (let ((next (disk-block-next disk-block)))
    ;; link block to available list
    (append-to-available disk-block swap-file)
    
    ;; any extending blocks are also deleted
    (if (> next 0)
        (unlink-block swap-file (open-block swap-file next))
        swap-file)))
  
(defun set-file-position-after-header (swap-file &optional (offset 0))
  "Sets swap file's file position after swap file header. If offset is
given, the offset is added to file position. The file position after
header is reserved for application meta data. Available meta data size
is block size reduced by header size."
  (file-position (swap-file-stream swap-file) (+ (swap-file-header-size) offset)))
