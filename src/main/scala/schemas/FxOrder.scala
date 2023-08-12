package schemas

case class FxOrder (
          order_id: String,
          user_name: String,
          order_time: String,
          order_type: String,
          quantity: String,
          price: String
        )


case class MatchingRecord (
                     source_order_id: String,
                     target_order_id: String,
                     order_time: String,
                     quantity: String,
                     price: String
                   )
