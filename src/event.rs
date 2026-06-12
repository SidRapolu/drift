use serde::{ Deserialize, Serialize };

// Explicit type for event time in u64 to differentiate from other potential u6 values in data
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventTime(pub u64);

// Two streams for current use case (would use majority logic for N streams)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamId {
    // Exchange Feed
    A,
    // Order Book
    B,
}

// Minimum Viable Payload for specific use case
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookUpdate {
    pub price_level: i64,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    pub event_time: EventTime,
    pub stream: StreamId,
    pub update: BookUpdate,
}

impl Event {
    // Constructor for cleaner tests
    pub fn new(event_time: u64, stream: StreamId, price_level: i64, size: u64) -> Self {
        Event {
            event_time: EventTime(event_time),
            stream,
            update: BookUpdate { price_level, size },
        }
    }
}

//Tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_times_order_like_integers() {
        assert!(EventTime(5) > EventTime(3));
        assert!(EventTime(3) < EventTime(5));
        assert_eq!(EventTime(7), EventTime(7));
    }

    #[test]
    fn equal_updates_compare_equal() {
        let x = BookUpdate { price_level: 100, size: 50 };
        let y = BookUpdate { price_level: 100, size: 50 };
        let z = BookUpdate { price_level: 100, size: 51 };
        assert_eq!(x, y);
        assert_ne!(x, z); // one-unit size difference IS divergence
    }

    #[test]
    fn constructor_builds_the_event_we_expect() {
        let e = Event::new(42, StreamId::A, 100, 50);
        assert_eq!(e.event_time, EventTime(42));
        assert_eq!(e.stream, StreamId::A);
        assert_eq!(e.update.price_level, 100);
        assert_eq!(e.update.size, 50);
    }
}
