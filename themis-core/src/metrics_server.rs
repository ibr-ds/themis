use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use prometheus::{self, Encoder, TextEncoder};

// start the warp server to provide the metrics
pub async fn start_server(port: u16) {
    let addr = ([0, 0, 0, 0], port).into();

    tracing::info!(?addr, "serving prometheus");

    Server::bind(&addr)
        .serve(make_service_fn(|_| async {
            Ok::<_, hyper::Error>(service_fn(serve_req))
        }))
        .await
        .unwrap()
}

async fn serve_req(req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    if req.uri().path() != "/metrics" {
        let response = Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();

        return Ok(response);
    }

    let encoder = TextEncoder::new();

    let metric_families = prometheus::gather();
    let mut buffer = Vec::with_capacity(4096);
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}
