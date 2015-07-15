(ns hildebrand.internal.util)

(defn throw-empty [x]
  (assert (not (empty? x)))
  x)

(def type-aliases-out
  {:string     :S
   :number     :N
   :list       :L
   :binary     :B
   :number-set :NS
   :string-set :SS
   :binary-set :BS
   :map        :M
   :null       :NULL
   :boolean    :BOOL})

(def type-aliases-in
  (into {}
    (for [[k v] type-aliases-out]
      [v k])))

(defn defmulti-dispatch [method v->handler]
  (doseq [[v handler] v->handler]
    (defmethod method v [& args] (apply handler args))))
