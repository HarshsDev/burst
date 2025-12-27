use rusoto_core::RusotoError;
use ssh2::Session;
use std::future::Future;
use std::io;
use std::io::Read;
use std::net::TcpStream;
use std::path::Path;
use std::{collections::HashMap, fmt::Error};
use tokio::time::{self, sleep, Duration};

use rusoto_ec2::{
    CancelSpotInstanceRequestsResult, DescribeInstancesResult, DescribeSpotInstanceRequestsResult,
    Ec2, RequestSpotInstancesError, RequestSpotInstancesResult, Reservation,
    TerminateInstancesResult,
};

pub struct SshConnection {}
struct Burst {}

pub struct MachineSetup {
    instance_type: String,
    ami: String,
    setup: Box<dyn Fn(&mut SshConnection) -> io::Result<()>>,
}

pub struct Machine {
    ssh: Option<SshConnection>,
    instance_type: String,
    ip: String,
    dns: String,
}

pub struct BurstBuilder {
    descriptors: HashMap<String, (MachineSetup, u32)>,
    max_duration: i64,
}

impl MachineSetup {
    pub fn new<F>(instance_type: String, ami: String, setup: F) -> Self
    where
        F: Fn(&mut SshConnection) -> io::Result<()> + 'static,
    {
        MachineSetup {
            instance_type,
            ami,
            setup: Box::new(setup),
        }
    }
}

impl Default for BurstBuilder {
    fn default() -> Self {
        BurstBuilder {
            descriptors: Default::default(),
            max_duration: 60,
        }
    }
}

pub async fn ssh_into_instance(
    dns: &str,
) -> Result<(), rusoto_core::RusotoError<rusoto_ec2::DescribeInstancesError>> {
    let dns = dns.to_string();
    tokio::task::spawn_blocking(move || -> Result<(), String> {
        let addr = format!("{}:22", dns);
        let key_path = Path::new(r"D:\Downloads\FirstOne (1).pem");

        for attempt in 0..10 {
            match TcpStream::connect(&addr) {
                Ok(tcp) => {
                    let mut sess = Session::new().unwrap();
                    sess.set_tcp_stream(tcp);
                    sess.set_timeout(20_000);
                    sess.handshake()
                        .map_err(|e| format!("SSH handshake failed: {}", e))?;
                    sess.userauth_pubkey_file("ec2-user", None, key_path, None)
                        .map_err(|e| format!("SSH auth failed: {}", e))?;
                    if !sess.authenticated() {
                        return Err("Sess auth failed".into());
                    }
                    let mut channel = sess
                        .channel_session()
                        .map_err(|e| format!("chaneel open failed: {}", e))?;
                    channel
                        .exec("uname -a")
                        .map_err(|e| format!("Command exec failed: {}", e))?;

                    let mut output = String::new();
                    channel.read_to_string(&mut output).unwrap();
                    channel.wait_close().unwrap();
                    println!("SSH OK: {}", output);
                    return Ok(());
                }
                Err(e) => {
                    eprintln!("SSH attempt {} failed: {}", attempt, e);

                    std::thread::sleep(std::time::Duration::from_secs(5));
                }
            }
        }
        Err("SSH failed after retries".into())
    })
    .await
    .map_err(|e| rusoto_core::RusotoError::ParseError(e.to_string()))?
    .map_err(|e| rusoto_core::RusotoError::ParseError(e))?;

    Ok(())
}

impl BurstBuilder {
    pub fn add_set(&mut self, name: String, number: u32, setup: MachineSetup)
    // where
    //     F: Fn(&mut SshConnection) -> io::Result<()>,
    {
        self.descriptors.insert(name, (setup, number));
    }

    pub fn set_max_duration(&mut self, hours: u8) {
        self.max_duration = hours as i64 * 60;
    }

    pub async fn run<F>(
        self,
        f: F,
    ) -> Result<Vec<String>, rusoto_core::RusotoError<RequestSpotInstancesError>>
    where
        F: FnOnce(HashMap<String, &mut [Machine]>) -> io::Result<()>,
    {
        let ec2 = rusoto_ec2::Ec2Client::new(rusoto_core::Region::EuNorth1);
        let mut spot_req_ids = Vec::new();
        for (name, (setup, number)) in &self.descriptors {
            let mut launch = rusoto_ec2::RequestSpotLaunchSpecification::default();
            launch.image_id = Some(setup.ami.to_string());
            launch.instance_type = Some(setup.instance_type.to_string());
            launch.key_name = Some(String::from("FirstOne"));
            launch.security_group_ids = Some(vec![String::from("sg-0701f648255d191ef")]);
            launch.subnet_id = Some(String::from("subnet-0801b2f255ddbfc93"));
            //launch.security_groups = Some(vec![String::from("Hello")]);

            //       println!("launch spec: {:?}", launch);
            let mut req = rusoto_ec2::RequestSpotInstancesRequest::default();
            req.instance_count = Some(i64::from(*number));
            req.block_duration_minutes = None;
            req.launch_specification = Some(launch);

            let mut request_result: Option<RequestSpotInstancesResult> = None;
            let mut last_err: Option<
                rusoto_core::RusotoError<rusoto_ec2::RequestSpotInstancesError>,
            > = None;

            for attempt in 0..5 {
                match ec2.request_spot_instances(req.clone()).await {
                    Ok(res) => {
                        println!(
                            "RequestSpotInstancesResult (attempt {}) = {:?}",
                            attempt, res
                        );
                        request_result = Some(res);
                        break;
                    }
                    Err(e) => {
                        eprintln!("request_spot_instances attempt {} failed: {}", attempt, e);
                        last_err = Some(e);
                        // back off before retrying
                        let backoff_secs = 2u64.pow(attempt).saturating_mul(2);
                        sleep(Duration::from_secs(std::cmp::min(backoff_secs, 30))).await;
                    }
                }
            }

            let res = if let Some(r) = request_result {
                r
            } else {
                // return the most-recent rusoto error (matches your function return type)
                return Err(last_err.unwrap());
            };
            // println!("Request spot instance request is {}", format!("{:?}", req));
            // let res: RequestSpotInstancesResult = ec2
            //     .request_spot_instances(req)
            //     .await
            //     .map_err(|e| RusotoError::ParseError(e.to_string()))?;
            // println!(
            //     "RequestSpotInstancesResult......{}",
            //     format!("{:?}", res.clone())
            // );
            spot_req_ids.extend(
                res.spot_instance_requests
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|f| f.spot_instance_request_id),
            );
        }

        let mut req = rusoto_ec2::DescribeSpotInstanceRequestsRequest::default();
        //     println!("DescribeSpotInstanceRequestsRequest.........................................df");
        req.spot_instance_request_ids = Some(spot_req_ids.clone());
        let instances: Vec<_>;
        loop {
            let res: DescribeSpotInstanceRequestsResult = ec2
                .describe_spot_instance_requests(req.clone())
                .await
                .map_err(|e| RusotoError::ParseError(e.to_string()))?;
            // for sir in res.spot_instance_requests.clone().unwrap_or_default() {
            //     println!("---- Spot request {:?} ----", sir.spot_instance_request_id);
            //     println!("state: {:?}", sir.state);
            //     println!("status: {:?}", sir.status);
            //     println!("launch_specification: {:?}", sir.launch_specification);
            //     println!("fault: {:?}", sir.fault);
            //     println!("tags: {:?}", sir.tags);
            // }
            let spot_request = res.spot_instance_requests.unwrap();
            let any_open = spot_request
                .iter()
                .any(|sir| sir.state.clone().unwrap() == "open");
            if !any_open {
                instances = spot_request
                    .into_iter()
                    .filter_map(|sir| sir.instance_id)
                    .collect();
                break;
            }
        }

        let mut cancel = rusoto_ec2::CancelSpotInstanceRequestsRequest::default();
        cancel.spot_instance_request_ids = spot_req_ids.clone();
        let res: CancelSpotInstanceRequestsResult = ec2
            .cancel_spot_instance_requests(cancel)
            .await
            .map_err(|e| RusotoError::ParseError(e.to_string()))?;
        let cancel_Res = res.cancelled_spot_instance_requests.unwrap();

        let mut any_not_ready = true;
        let mut machines = Vec::new();
        let mut req = rusoto_ec2::DescribeInstancesRequest::default();
        req.instance_ids = Some(instances.clone());
        while any_not_ready {
            any_not_ready = false;
            //   machines.clear();
            let res: DescribeInstancesResult = ec2
                .describe_instances(req.clone())
                .await
                .map_err(|e| RusotoError::ParseError(e.to_string()))?;

            for reservation in res.reservations.unwrap() {
                for instance in reservation.instances.unwrap() {
                    match instance {
                        rusoto_ec2::Instance {
                            instance_type: Some(instance_type),
                            private_ip_address: Some(ip),
                            public_dns_name: Some(dns),
                            ..
                        } => {
                            let machine = Machine {
                                ssh: None,
                                instance_type,
                                ip,
                                dns,
                            };
                            machines.push(machine);
                        }
                        _ => {
                            any_not_ready = true;
                        }
                    }
                }
            }
            if any_not_ready {
                // sleep a bit before retrying (use tokio::time::sleep since run() is async)
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }

        for machine in &machines {
            //      ssh_into_instance(&machine.dns);
            let addr = &format!("{}:22", machine.dns);
            println!("Trying to resolve/connect to: {}", addr);

            let mut port_open = false;
            let max_port_checks = 20;
            for attempt in 1..=max_port_checks {
                match time::timeout(
                    Duration::from_secs(5),
                    tokio::net::TcpStream::connect(&addr),
                )
                .await
                {
                    Ok(Ok(_stream)) => {
                        port_open = true;
                        eprintln!("Port 22 open on {} (attempt{})", addr, attempt);
                        break;
                    }
                    Ok(Err(e)) => {
                        eprintln!("Attempt{}: connect error: {}", attempt, e);
                    }
                    Err(_) => {
                        eprintln!("Attempt{}: connect timed out", attempt);
                    }
                }
                time::sleep(Duration::from_secs(3)).await;
            }
            if !port_open {
                return Err(RusotoError::ParseError(format!(
                    "Timed out waiting for SSH port on {}",
                    addr
                )));
            }

            let addrs = format!("{}:22", machine.dns);
            let key_path = Path::new(r"D:\Downloads\FirstOne (1).pem");
            let command = "ip addr show up".to_string();

            // run blocking TCP + SSH work on a blocking thread and retry a few times
            let ssh_result = tokio::task::spawn_blocking(move || -> Result<String, String> {
                // number of handshake attempts (tune if needed)
                for attempt in 1..=6 {
                    // blocking TCP connect
                    match TcpStream::connect(&addrs) {
                        Ok(tcp) => {
                            // create session
                            let mut sess = Session::new().unwrap();
                            sess.set_tcp_stream(tcp);
                            // increase timeout (milliseconds)
                            sess.set_timeout(240_000);

                            // attempt handshake
                            match sess.handshake() {
                                Ok(_) => {
                                    // try auth
                                    sess.userauth_pubkey_file("ec2-user", None, &key_path, None)
                                        .map_err(|e| format!("SSH auth failed: {}", e))?;

                                    if !sess.authenticated() {
                                        return Err("SSH authentication failed".to_string());
                                    }

                                    // run command and capture output
                                    let mut channel = sess
                                        .channel_session()
                                        .map_err(|e| format!("open channel failed: {}", e))?;
                                    channel
                                        .exec(&command)
                                        .map_err(|e| format!("exec failed: {}", e))?;
                                    let mut out = String::new();
                                    channel
                                        .read_to_string(&mut out)
                                        .map_err(|e| format!("read failed: {}", e))?;
                                    channel
                                        .wait_close()
                                        .map_err(|e| format!("wait_close failed: {}", e))?;
                                    let status = channel.exit_status().unwrap_or(-1);
                                    if status != 0 {
                                        return Err(format!(
                                            "remote command exited with status {}",
                                            status
                                        ));
                                    }
                                    return Ok(out);
                                }
                                Err(e) => {
                                    // handshake failed (transient): retry after short sleep
                                    let err_text =
                                        format!("handshake attempt {} failed: {}", attempt, e);
                                    if attempt == 6 {
                                        return Err(err_text);
                                    } else {
                                        std::thread::sleep(std::time::Duration::from_secs(2));
                                        continue;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // TCP connect failed (transient): retry
                            let err_text = format!("tcp connect attempt {} failed: {}", attempt, e);
                            if attempt == 6 {
                                return Err(err_text);
                            } else {
                                std::thread::sleep(std::time::Duration::from_secs(2));
                                continue;
                            }
                        }
                    }
                }
                Err("ssh attempts exhausted".to_string())
            })
            .await
            .map_err(|e| RusotoError::ParseError(format!("spawn_blocking join error: {}", e)))?; // map JoinError

            // unwrap inner result or return rusto error
            let output = match ssh_result {
                Ok(s) => s,
                Err(err_msg) => {
                    return Err(RusotoError::ParseError(format!(
                        "SSH failed for {}: {}",
                        addr.clone(),
                        err_msg
                    )));
                }
            };

            println!("SSH command output on {}:\n{}", addr, output);
            // let tcp = TcpStream::connect(&format!("{}:22", machine.dns)).unwrap();
            // let privatekeypath = Path::new(r"D:\Downloads\FirstOne (1).pem");
            // let mut sess = Session::new().unwrap();
            // sess.set_tcp_stream(tcp);
            // sess.set_timeout(30_000);

            // sess.handshake().unwrap();
            // sess.userauth_pubkey_file("ec2-user", None, privatekeypath, None)
            //     .map_err(|e| RusotoError::ParseError(format!("SSH auth failed: {}", e)))?;
            // // sess.userauth_agent("ec2-user").unwrap();

            // if !sess.authenticated() {
            //     return Err(RusotoError::ParseError(format!(
            //         "SSH auth failed for............"
            //     )));
            // }

            // let mut channel = sess.channel_session().unwrap();
            // channel.exec("ip addr show up").unwrap();
            // let mut s = String::new();
            // channel.read_to_string(&mut s).unwrap();
            // println!("{}", s);
            // channel.wait_close().unwrap();
            // println!("{}", channel.exit_status().unwrap());
        }

        let mut terminate_instances_request = rusoto_ec2::TerminateInstancesRequest::default();
        terminate_instances_request.instance_ids = req.instance_ids.unwrap();
        let res: TerminateInstancesResult = ec2
            .terminate_instances(terminate_instances_request)
            .await
            .map_err(|e| RusotoError::ParseError(e.to_string()))?;
        Ok(spot_req_ids)
    }
}
