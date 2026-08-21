use rust_decimal::Decimal;

use crate::error::DomainError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookLevel {
    pub price: Decimal,
    pub amount: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookAnalytics {
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub mid_price: Decimal,
    pub spread: Decimal,
    pub spread_bps: Decimal,
    pub bid_depth: Decimal,
    pub ask_depth: Decimal,
    pub imbalance: Option<Decimal>,
    pub depth_levels: usize,
}

pub fn calculate(
    bids: &[BookLevel],
    asks: &[BookLevel],
    depth_levels: usize,
) -> Result<BookAnalytics, DomainError> {
    if depth_levels == 0 {
        return Err(DomainError::InvalidDepth);
    }
    validate_levels(bids)?;
    validate_levels(asks)?;

    let best_bid = bids
        .iter()
        .map(|level| level.price)
        .max()
        .ok_or(DomainError::EmptyBids)?;
    let best_ask = asks
        .iter()
        .map(|level| level.price)
        .min()
        .ok_or(DomainError::EmptyAsks)?;

    if best_bid >= best_ask {
        return Err(DomainError::CrossedBook { best_bid, best_ask });
    }

    let two = Decimal::from(2_u32);
    let ten_thousand = Decimal::from(10_000_u32);
    let mid_price = best_bid
        .checked_add(best_ask)
        .and_then(|value| value.checked_div(two))
        .ok_or(DomainError::DecimalOverflow)?;
    let spread = best_ask
        .checked_sub(best_bid)
        .ok_or(DomainError::DecimalOverflow)?;
    let spread_bps = spread
        .checked_div(mid_price)
        .and_then(|value| value.checked_mul(ten_thousand))
        .ok_or(DomainError::DecimalOverflow)?;

    let mut sorted_bids = bids.to_vec();
    sorted_bids.sort_by_key(|level| std::cmp::Reverse(level.price));
    let mut sorted_asks = asks.to_vec();
    sorted_asks.sort_by_key(|level| level.price);

    // Both sides use the same effective level count so depth and imbalance
    // never compare different portions of an asymmetric book.
    let effective_depth = depth_levels.min(sorted_bids.len()).min(sorted_asks.len());
    let bid_depth = sum_amounts(sorted_bids.iter().take(effective_depth))?;
    let ask_depth = sum_amounts(sorted_asks.iter().take(effective_depth))?;
    let total_depth = bid_depth
        .checked_add(ask_depth)
        .ok_or(DomainError::DecimalOverflow)?;
    let imbalance = if total_depth.is_zero() {
        None
    } else {
        Some(
            bid_depth
                .checked_sub(ask_depth)
                .and_then(|value| value.checked_div(total_depth))
                .ok_or(DomainError::DecimalOverflow)?,
        )
    };

    Ok(BookAnalytics {
        best_bid,
        best_ask,
        mid_price,
        spread,
        spread_bps,
        bid_depth,
        ask_depth,
        imbalance,
        depth_levels: effective_depth,
    })
}

fn validate_levels(levels: &[BookLevel]) -> Result<(), DomainError> {
    for level in levels {
        if level.price <= Decimal::ZERO {
            return Err(DomainError::InvalidLevel(format!(
                "price must be positive, got {}",
                level.price
            )));
        }
        if level.amount < Decimal::ZERO {
            return Err(DomainError::InvalidLevel(format!(
                "amount must not be negative, got {}",
                level.amount
            )));
        }
    }
    Ok(())
}

fn sum_amounts<'a>(
    mut levels: impl Iterator<Item = &'a BookLevel>,
) -> Result<Decimal, DomainError> {
    levels.try_fold(Decimal::ZERO, |sum, level| {
        sum.checked_add(level.amount)
            .ok_or(DomainError::DecimalOverflow)
    })
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn level(price: Decimal, amount: Decimal) -> BookLevel {
        BookLevel { price, amount }
    }

    #[test]
    fn calculates_best_prices_mid_spread_depth_and_positive_imbalance() {
        let bids = vec![
            level(dec!(100.10), dec!(3.0000)),
            level(dec!(100.00), dec!(2)),
        ];
        let asks = vec![level(dec!(100.30), dec!(1)), level(dec!(100.40), dec!(1))];
        let analytics = calculate(&bids, &asks, 10).expect("valid fixture");

        assert_eq!(analytics.best_bid, dec!(100.10));
        assert_eq!(analytics.best_ask, dec!(100.30));
        assert_eq!(analytics.mid_price, dec!(100.20));
        assert_eq!(analytics.spread, dec!(0.20));
        assert_eq!(
            analytics.spread_bps,
            dec!(0.20) / dec!(100.20) * dec!(10000)
        );
        assert_eq!(analytics.bid_depth, dec!(5.0000));
        assert_eq!(analytics.ask_depth, dec!(2));
        assert!(analytics
            .imbalance
            .is_some_and(|value| value > Decimal::ZERO));
        assert_eq!(analytics.depth_levels, 2);
    }

    #[test]
    fn calculates_negative_imbalance() {
        let result = calculate(&[level(dec!(10), dec!(1))], &[level(dec!(11), dec!(3))], 10)
            .expect("valid fixture");
        assert_eq!(result.imbalance, Some(dec!(-0.5)));
    }

    #[test]
    fn zero_total_volume_has_no_imbalance() {
        let result = calculate(
            &[level(dec!(10), Decimal::ZERO)],
            &[level(dec!(11), Decimal::ZERO)],
            10,
        )
        .expect("valid zero-depth fixture");
        assert_eq!(result.imbalance, None);
    }

    #[test]
    fn rejects_empty_bid_side() {
        assert_eq!(
            calculate(&[], &[level(dec!(11), dec!(1))], 10),
            Err(DomainError::EmptyBids)
        );
    }

    #[test]
    fn rejects_empty_ask_side() {
        assert_eq!(
            calculate(&[level(dec!(10), dec!(1))], &[], 10),
            Err(DomainError::EmptyAsks)
        );
    }

    #[test]
    fn rejects_crossed_book() {
        assert!(matches!(
            calculate(&[level(dec!(11), dec!(1))], &[level(dec!(10), dec!(1))], 10),
            Err(DomainError::CrossedBook { .. })
        ));
    }

    #[test]
    fn uses_only_top_n_levels() {
        let result = calculate(
            &[
                level(dec!(10), dec!(1)),
                level(dec!(9), dec!(2)),
                level(dec!(8), dec!(100)),
            ],
            &[
                level(dec!(11), dec!(3)),
                level(dec!(12), dec!(4)),
                level(dec!(13), dec!(100)),
            ],
            2,
        )
        .expect("valid fixture");
        assert_eq!(result.bid_depth, dec!(3));
        assert_eq!(result.ask_depth, dec!(7));
        assert_eq!(result.depth_levels, 2);
    }

    #[test]
    fn asymmetric_books_use_the_same_effective_depth_on_both_sides() {
        let result = calculate(
            &[
                level(dec!(10), dec!(1)),
                level(dec!(9), dec!(2)),
                level(dec!(8), dec!(100)),
            ],
            &[level(dec!(11), dec!(3)), level(dec!(12), dec!(4))],
            10,
        )
        .expect("valid asymmetric fixture");

        assert_eq!(result.depth_levels, 2);
        assert_eq!(result.bid_depth, dec!(3));
        assert_eq!(result.ask_depth, dec!(7));
    }

    #[test]
    fn preserves_decimal_precision() {
        let result = calculate(
            &[level(dec!(117231.20), dec!(0.00000001))],
            &[level(dec!(117235.60), dec!(0.00000002))],
            10,
        )
        .expect("valid precise fixture");
        assert_eq!(result.mid_price, dec!(117233.40));
        assert_eq!(result.spread, dec!(4.40));
        assert_eq!(result.bid_depth, dec!(0.00000001));
    }

    #[test]
    fn rejects_negative_amount_as_invalid_payload() {
        assert!(matches!(
            calculate(
                &[level(dec!(10), dec!(-1))],
                &[level(dec!(11), dec!(1))],
                10
            ),
            Err(DomainError::InvalidLevel(_))
        ));
    }
}
