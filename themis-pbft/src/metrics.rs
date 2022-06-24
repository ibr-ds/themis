#[cfg(feature = "metrics")]
use lazy_static::lazy_static;
#[cfg(feature = "metrics")]
use prometheus::*;

#[cfg(feature = "metrics")]
lazy_static! {
    static ref REQUEST_TIMES: Histogram = register_histogram!(HistogramOpts::new(
        "request_execution_times",
        "Request Execution Times"
    )
    .buckets(vec![
        0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
    ]))
    .unwrap();
    static ref VIEW_CHANGE_TIMES: Histogram = register_histogram!(HistogramOpts::new(
        "view_change_execution_times",
        "View Change Execution Times"
    )
    .buckets(vec![
        0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
    ]))
    .unwrap();
    static ref LOW_MARK: IntGauge =
        register_int_gauge!(opts!("low_mark", "Request Id Low Mark")).unwrap();
    static ref HIGH_MARK: IntGauge =
        register_int_gauge!(opts!("high_mark", "Request Id High Mark")).unwrap();
    static ref NEXT_SEQUENCE: IntGauge =
        register_int_gauge!(opts!("next_sequence", "Next Sequence Number")).unwrap();
    static ref LAST_COMMIT: IntGauge =
        register_int_gauge!(opts!("last_commit", "Last committed sequence number")).unwrap();
    static ref VIEW_NUMBER: IntGauge =
        register_int_gauge!(opts!("view_number", "The number of the current view")).unwrap();
    static ref IS_PRIMARY: IntGauge = register_int_gauge!(opts!(
        "is_primary",
        "Is this replica the primary? 1=true, 0=false"
    ))
    .unwrap();
}

#[derive(Debug)]
pub struct Timer {
    #[cfg(feature = "metrics")]
    timer: Option<HistogramTimer>,
}

#[cfg(feature = "metrics")]
impl Timer {
    pub fn new(histogram_timer: HistogramTimer) -> Self {
        Self {
            timer: Some(histogram_timer),
        }
    }

    // timer will observer automatically when going out of scope
    pub fn stop_and_record(&mut self) {
        let option = self.timer.take();
        if let Some(histogram_timer) = option {
            histogram_timer.stop_and_record();
        }
    }

    // discard to prevent observation when going out of scope
    pub fn stop_and_discard(&mut self) {
        let option = self.timer.take();
        if let Some(histogram_timer) = option {
            histogram_timer.stop_and_discard();
        }
    }
}

#[cfg(not(feature = "metrics"))]
impl Timer {
    pub fn stop_and_record(&mut self) {
        //do nothing
    }

    pub fn stop_and_discard(&mut self) {
        //do nothing
    }
}

#[cfg(feature = "metrics")]
pub fn request_execution_timer() -> Timer {
    Timer::new(REQUEST_TIMES.start_timer())
}
#[cfg(not(feature = "metrics"))]
pub fn request_execution_timer() -> Timer {
    Timer {}
}

#[cfg(feature = "metrics")]
pub fn view_change_timer() -> Timer {
    Timer::new(VIEW_CHANGE_TIMES.start_timer())
}
#[cfg(not(feature = "metrics"))]
pub fn view_change_timer() -> Timer {
    Timer {}
}

#[cfg(feature = "metrics")]
pub fn record_low_mark(mark: u64) {
    LOW_MARK.set(mark as i64);
}
#[cfg(not(feature = "metrics"))]
pub fn record_low_mark(_sequence_number: u64) {
    //do nothing
    //compiler will nicely delete this function
}

#[cfg(feature = "metrics")]
pub fn record_high_mark(mark: u64) {
    HIGH_MARK.set(mark as i64);
}
#[cfg(not(feature = "metrics"))]
pub fn record_high_mark(_sequence_number: u64) {
    //do nothing
    //compiler will nicely delete this function
}

#[cfg(feature = "full-metrics")]
pub fn record_next_sequence(sequence_number: u64) {
    NEXT_SEQUENCE.set(sequence_number as i64);
}
#[cfg(not(feature = "full-metrics"))]
pub fn record_next_sequence(_sequence_number: u64) {
    //do nothing
    //compiler will nicely delete this function
}

#[cfg(feature = "full-metrics")]
pub fn record_last_commit(sequence_number: u64) {
    LAST_COMMIT.set(sequence_number as i64);
}
#[cfg(not(feature = "full-metrics"))]
pub fn record_last_commit(_sequence_number: u64) {
    //do nothing
    //compiler will nicely delete this function
}

#[cfg(feature = "metrics")]
pub fn record_view_number(view: u64) {
    VIEW_NUMBER.set(view as i64);
}
#[cfg(not(feature = "metrics"))]
pub fn record_view_number(_view: u64) {
    //do nothing
    //compiler will nicely delete this function
}

#[cfg(feature = "metrics")]
pub fn record_is_primary(state: bool) {
    IS_PRIMARY.set(state as i64)
}
#[cfg(not(feature = "metrics"))]
pub fn record_is_primary(_state: bool) {
    //do nothing
    //compiler will nicely delete this function
}
