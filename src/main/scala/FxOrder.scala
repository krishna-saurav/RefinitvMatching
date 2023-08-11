case class FxOrder (
          order_id: String,
          user_name: String,
          order_time: Long,
          order_type: String,
          quantity: Int,
          price: Int
        )


case class MatchingRecord (
                     source_order_id: String,
                     target_order_id: String,
                     order_time: Long,
                     quantity: Int,
                     price: Int
                   )
