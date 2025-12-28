use rusoto_core::RusotoError;
use rusoto_ec2::{
    CancelSpotInstanceRequestsResult, DescribeInstancesResult, DescribeSpotInstanceRequestsResult,
    Ec2, RequestSpotInstancesError, RequestSpotInstancesResult, Reservation,
    TerminateInstancesResult,
};
use ssh2::Session;
use std::future::Future;
use std::hash::Hash;
use std::io;
use std::io::Read;
use std::net::TcpStream;
use std::net::{SocketAddr, TcpStream as StdTcpStream, ToSocketAddrs};
use std::path::Path;
use std::path::PathBuf;
use std::time::{Duration as StdDuration, Instant};
use std::{collections::HashMap, fmt::Error};
use tokio::process::Command;
use tokio::time::{self, sleep, Duration};
use tokio::time::{timeout, Duration as TokioDuration};

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
        let mut setup_fns = HashMap::new();
        let ec2 = rusoto_ec2::Ec2Client::new(rusoto_core::Region::EuNorth1);
        let mut spot_req_ids = Vec::new();
        let mut id_to_name = HashMap::new();

        for (name, (setup, number)) in &self.descriptors {
            let mut launch = rusoto_ec2::RequestSpotLaunchSpecification::default();
            launch.image_id = Some(setup.ami.to_string());
            launch.instance_type = Some(setup.instance_type.to_string());
            launch.key_name = Some(String::from("FirstOne"));
            launch.security_group_ids = Some(vec![String::from("sg-0701f648255d191ef")]);
            launch.subnet_id = Some(String::from("subnet-0801b2f255ddbfc93"));
            //launch.security_groups = Some(vec![String::from("Hello")]);
            setup_fns.insert(name, &setup.setup);
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
                    .filter_map(|f| f.spot_instance_request_id)
                    .map(|sir| {
                        id_to_name.insert(name.clone(), sir.clone());
                        sir
                    }),
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
                    .filter_map(|sir| {
                        if sir.spot_instance_request_id.is_some() {
                            let mut name = id_to_name.remove(&sir.spot_instance_request_id.unwrap());
                            id_to_name
                                .insert(sir.instance_id.as_ref().unwrap().clone(), String::from(name.get_or_insert(String::from("Server")).clone()));
                        }
                        sir.instance_id
                    })
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
        let mut machines = HashMap::new();
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
                            instance_id: Some(instance_id),
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
                            let name = &id_to_name[&instance_id];
                            //      println!("dns is :{}", machine.dns);
                            machines.entry(name).or_insert_with(Vec::new).push(machine);
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

        for (ref name, ref mut machine) in &mut machines {
            // Replace `addr` and `command` with your values; `addr` should be "<host>:22" or host only when passing ssh user@host
            let host = machine[0].dns.clone();
            // e.g. "ec2-16-..amazonaws.com"
        //    let f = &setup_fns[*name];
            let ssh_user = "ec2-user";
            let key_path = PathBuf::from(r"D:\Downloads\FirstOne (1).pem");
            let remote_command = "ip addr show up";

            // We'll attempt multiple times
            let mut last_err: Option<String> = None;
            let max_attempts = 6;
            let per_attempt_timeout = Duration::from_secs(30);

            for attempt in 1..=max_attempts {
                println!("ssh-cli attempt {} -> {}@{}", attempt, ssh_user, host);

                // Build the ssh command. Using -o IdentitiesOnly=yes ensures ssh uses the given key.
                // Using -o StrictHostKeyChecking=no is convenient for automation but less secure.
                // If you prefer to trust host keys, remove the StrictHostKeyChecking option.
                let mut cmd = Command::new("ssh");
                cmd.arg("-i")
                    .arg(key_path.as_os_str())
                    .arg("-o")
                    .arg("IdentitiesOnly=yes")
                    .arg("-o")
                    .arg("BatchMode=yes") // fail instead of interactive
                    .arg("-o")
                    .arg("StrictHostKeyChecking=no") // optional: avoids host key prompt
                    .arg(format!("{}@{}", ssh_user, host))
                    .arg(remote_command);

                // Run with timeout
                match timeout(per_attempt_timeout, cmd.output()).await {
                    Ok(Ok(output)) => {
                        if output.status.success() {
                            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                            println!("ssh-cli succeeded on attempt {}: {}", attempt, stdout);
                            last_err = None;
                            break;
                        } else {
                            let stderr = String::from_utf8_lossy(&output.stderr);
                            let stdout = String::from_utf8_lossy(&output.stdout);
                            let msg = format!(
                                "ssh exit non-zero (attempt {}): status={} stdout={} stderr={}",
                                attempt,
                                output.status.code().unwrap_or(-1),
                                stdout,
                                stderr
                            );
                            eprintln!("{}", msg);
                            last_err = Some(msg);
                        }
                    }
                    Ok(Err(e)) => {
                        // Child process spawn failed
                        let msg = format!("Failed to spawn ssh child (attempt {}): {}", attempt, e);
                        eprintln!("{}", msg);
                        last_err = Some(msg);
                    }
                    Err(_) => {
                        // Timeout
                        let msg = format!(
                            "ssh attempt {} timed out after {:?}",
                            attempt, per_attempt_timeout
                        );
                        eprintln!("{}", msg);
                        last_err = Some(msg);
                    }
                }

                // backoff before next attempt
                tokio::time::sleep(Duration::from_secs(3)).await;
            }

            if let Some(err_msg) = last_err {
                return Err(RusotoError::ParseError(format!(
                    "SSH (cli) failed for {}: {}",
                    host, err_msg
                )));
            }
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

// let addr = format!("{}:22", machine.dns);
// let key_path = PathBuf::from(r"D:\Downloads\FirstOne (1).pem");
// let cmd = "ip addr show up".to_string();

// // overall deadline for this machine's SSH attempts (tune if you want)
// let overall_deadline = Instant::now() + StdDuration::from_secs(180); // 3 minutes

// // Try several attempts; each attempt does a blocking connect+handshake inside spawn_blocking
// let mut last_err: Option<String> = None;
// for attempt in 1..=6 {
//     if Instant::now() > overall_deadline {
//         break;
//     }

//     println!("SSH attempt {} for {}", attempt, addr);

//     // Spawn a blocking task for a single attempt and bound it with a Tokio timeout
//     // (so the whole attempt cannot hang forever).
//     let addr_clone = addr.clone();
//     let key_clone = key_path.clone();
//     let cmd_clone = cmd.clone();

//     // Per-attempt timeout (how long we allow the attempt to run)
//     let per_attempt_timeout = TokioDuration::from_secs(30);

//     let handle = tokio::task::spawn_blocking(move || -> Result<String, String> {
//         // Resolve address (optional)
//         let sock_addr = addr_clone
//             .to_socket_addrs()
//             .map_err(|e| format!("resolve failed: {}", e))?
//             .next()
//             .ok_or_else(|| "no addresses found".to_string())?;

//         // Do a blocking connect with explicit timeout
//         let connect_timeout = StdDuration::from_secs(8);
//         let tcp = match StdTcpStream::connect_timeout(&sock_addr, connect_timeout) {
//             Ok(s) => s,
//             Err(e) => return Err(format!("tcp connect failed: {}", e)),
//         };

//         // set basic socket opts
//         let _ = tcp.set_nodelay(true);
//         let _ = tcp.set_read_timeout(Some(StdDuration::from_secs(60)));
//         let _ = tcp.set_write_timeout(Some(StdDuration::from_secs(60)));

//         // create SSH session
//         let mut sess = Session::new().unwrap();
//         sess.set_tcp_stream(tcp);
//         sess.set_timeout(120_000); // ms

//         // Attempt the handshake
//         sess.handshake()
//             .map_err(|e| format!("handshake failed: {}", e))?;

//         // Authenticate
//         sess.userauth_pubkey_file("ec2-user", None, &key_clone, None)
//             .map_err(|e| format!("userauth failed: {}", e))?;

//         if !sess.authenticated() {
//             return Err("ssh not authenticated".to_string());
//         }

//         // Open channel, run command, read output
//         let mut channel = sess
//             .channel_session()
//             .map_err(|e| format!("open channel: {}", e))?;
//         channel
//             .exec(&cmd_clone)
//             .map_err(|e| format!("exec failed: {}", e))?;
//         let mut out = String::new();
//         channel
//             .read_to_string(&mut out)
//             .map_err(|e| format!("read failed: {}", e))?;
//         channel
//             .wait_close()
//             .map_err(|e| format!("wait_close failed: {}", e))?;
//         let status = channel.exit_status().unwrap_or(-1);
//         if status != 0 {
//             return Err(format!("remote exit status {}", status));
//         }

//         Ok(out)
//     });

//     // Wait for the attempt but abort it if it exceeds per_attempt_timeout
//     match timeout(per_attempt_timeout, handle).await {
//         Ok(join_res) => match join_res {
//             Ok(inner_res) => match inner_res {
//                 Ok(output) => {
//                     println!("SSH succeeded for {} on attempt {}", addr, attempt);
//                     println!("SSH output:\n{}", output);
//                     last_err = None;
//                     break; // success
//                 }
//                 Err(err_msg) => {
//                     eprintln!("Attempt {} failed: {}", attempt, err_msg);
//                     last_err = Some(err_msg);
//                 }
//             },
//             Err(join_err) => {
//                 // spawn_blocking panicked or join error
//                 let msg = format!("spawn_blocking join error: {}", join_err);
//                 eprintln!("{}", msg);
//                 last_err = Some(msg);
//             }
//         },
//         Err(_) => {
//             let msg = format!(
//                 "Attempt {} timed out after {:?} for {}",
//                 attempt, per_attempt_timeout, addr
//             );
//             eprintln!("{}", msg);
//             last_err = Some(msg);
//         }
//     }

//     // small backoff between attempts
//     sleep(TokioDuration::from_secs(3)).await;
// }

// if let Some(err) = last_err {
//     return Err(RusotoError::ParseError(format!(
//         "SSH failed for {}: {}",
//         addr, err
//     )));
// }
// // //      ssh_into_instance(&machine.dns);
// // let addr = &format!("{}:22", machine.dns);
// // println!("Trying to resolve/connect to: {}", addr);

// // let mut port_open = false;
// // let max_port_checks = 20;
// // for attempt in 1..=max_port_checks {
// //     match time::timeout(
// //         Duration::from_secs(5),
// //         tokio::net::TcpStream::connect(&addr),
// //     )
// //     .await
// //     {
// //         Ok(Ok(_stream)) => {
// //             port_open = true;
// //             eprintln!("Port 22 open on {} (attempt{})", addr, attempt);
// //             break;
// //         }
// //         Ok(Err(e)) => {
// //             eprintln!("Attempt{}: connect error: {}", attempt, e);
// //         }
// //         Err(_) => {
// //             eprintln!("Attempt{}: connect timed out", attempt);
// //         }
// //     }
// //     time::sleep(Duration::from_secs(3)).await;
// // }
// // if !port_open {
// //     return Err(RusotoError::ParseError(format!(
// //         "Timed out waiting for SSH port on {}",
// //         addr
// //     )));
// // }

// // let addrs = format!("{}:22", machine.dns);
// // let key_path = Path::new(r"D:\Downloads\FirstOne (1).pem");
// // let command = "ip addr show up".to_string();

// // // run blocking TCP + SSH work on a blocking thread and retry a few times
// // let ssh_result = tokio::task::spawn_blocking(move || -> Result<String, String> {
// //     // number of handshake attempts (tune if needed)
// //     for attempt in 1..=6 {
// //         // blocking TCP connect
// //         match TcpStream::connect(&addrs) {
// //             Ok(tcp) => {
// //                 // create session
// //                 let mut sess = Session::new().unwrap();
// //                 sess.set_tcp_stream(tcp);
// //                 // increase timeout (milliseconds)
// //                 sess.set_timeout(240_000);

// //                 // attempt handshake
// //                 match sess.handshake() {
// //                     Ok(_) => {
// //                         // try auth
// //                         sess.userauth_pubkey_file("ec2-user", None, &key_path, None)
// //                             .map_err(|e| format!("SSH auth failed: {}", e))?;

// //                         if !sess.authenticated() {
// //                             return Err("SSH authentication failed".to_string());
// //                         }

// //                         // run command and capture output
// //                         let mut channel = sess
// //                             .channel_session()
// //                             .map_err(|e| format!("open channel failed: {}", e))?;
// //                         channel
// //                             .exec(&command)
// //                             .map_err(|e| format!("exec failed: {}", e))?;
// //                         let mut out = String::new();
// //                         channel
// //                             .read_to_string(&mut out)
// //                             .map_err(|e| format!("read failed: {}", e))?;
// //                         channel
// //                             .wait_close()
// //                             .map_err(|e| format!("wait_close failed: {}", e))?;
// //                         let status = channel.exit_status().unwrap_or(-1);
// //                         if status != 0 {
// //                             return Err(format!(
// //                                 "remote command exited with status {}",
// //                                 status
// //                             ));
// //                         }
// //                         return Ok(out);
// //                     }
// //                     Err(e) => {
// //                         // handshake failed (transient): retry after short sleep
// //                         let err_text =
// //                             format!("handshake attempt {} failed: {}", attempt, e);
// //                         if attempt == 6 {
// //                             return Err(err_text);
// //                         } else {
// //                             std::thread::sleep(std::time::Duration::from_secs(2));
// //                             continue;
// //                         }
// //                     }
// //                 }
// //             }
// //             Err(e) => {
// //                 // TCP connect failed (transient): retry
// //                 let err_text = format!("tcp connect attempt {} failed: {}", attempt, e);
// //                 if attempt == 6 {
// //                     return Err(err_text);
// //                 } else {
// //                     std::thread::sleep(std::time::Duration::from_secs(2));
// //                     continue;
// //                 }
// //             }
// //         }
// //     }
// //     Err("ssh attempts exhausted".to_string())
// // })
// // .await
// // .map_err(|e| RusotoError::ParseError(format!("spawn_blocking join error: {}", e)))?; // map JoinError

// // // unwrap inner result or return rusto error
// // let output = match ssh_result {
// //     Ok(s) => s,
// //     Err(err_msg) => {
// //         return Err(RusotoError::ParseError(format!(
// //             "SSH failed for {}: {}",
// //             addr.clone(),
// //             err_msg
// //         )));
// //     }
// // };

// // println!("SSH command output on {}:\n{}", addr, output);
// // let tcp = TcpStream::connect(&format!("{}:22", machine.dns)).unwrap();
// // let privatekeypath = Path::new(r"D:\Downloads\FirstOne (1).pem");
// // let mut sess = Session::new().unwrap();
// // sess.set_tcp_stream(tcp);
// // sess.set_timeout(30_000);

// // sess.handshake().unwrap();
// // sess.userauth_pubkey_file("ec2-user", None, privatekeypath, None)
// //     .map_err(|e| RusotoError::ParseError(format!("SSH auth failed: {}", e)))?;
// // // sess.userauth_agent("ec2-user").unwrap();

// // if !sess.authenticated() {
// //     return Err(RusotoError::ParseError(format!(
// //         "SSH auth failed for............"
// //     )));
// // }

// // let mut channel = sess.channel_session().unwrap();
// // channel.exec("ip addr show up").unwrap();
// // let mut s = String::new();
// // channel.read_to_string(&mut s).unwrap();
// // println!("{}", s);
// // channel.wait_close().unwrap();
// // println!("{}", channel.exit_status().unwrap());
