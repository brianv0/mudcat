//! The mudcat server and command-line interface.

use clap::{Parser, Subcommand};
use mudcat::instance::{Instance, Principal};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "mudcat", version, about = "mudcat: a multimodal data catalog")]
struct Cli {
    /// The catalog database.
    #[arg(long, env = "MUDCAT_DB", default_value = "postgres://mudcat:mudcat@localhost:54329/mudcat")]
    db: String,
    /// The directory for the instance secret, the manifest store and the simulated storage.
    #[arg(long, env = "MUDCAT_DATA", default_value = "target/mudcat-data")]
    data: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Apply the schema to the database.
    Init,
    /// Serve the HTTP interfaces.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        addr: String,
    },
    /// Create a tenant with an administrator.
    CreateTenant { name: String, #[arg(long, default_value = "user:admin")] admin: String },
    /// List the tenants of the instance.
    Tenants,
    /// Issue a token for a principal, for the endpoint of a tenant.
    Token { tenant: String, principal: String, #[arg(long, default_value_t = 8)] hours: i64 },
    /// Move a tenant to another database.
    MoveTenant { tenant: String, database: String },
    /// Remove the rows that a tenant left in a database, when the report of a move said that they stayed.
    RemoveLeftRows { tenant: String, database: String },
    /// Print the authorization schema for an external service (spicedb or openfga).
    AuthzSchema { target: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if let Cmd::AuthzSchema { target } = &cli.cmd {
        print!("{}", mudcat::authz::neutral_schema(target));
        return Ok(());
    }
    let inst = Instance::open(&cli.db, &cli.data).await?;
    match cli.cmd {
        Cmd::Init => println!("schema applied"),
        Cmd::Serve { addr } => {
            let app = mudcat::api::router(inst.clone());
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            eprintln!("mudcat listens on http://{addr}; tenants are served at <name>.{}", inst.config.endpoint_domain);
            let relay_inst = inst.clone();
            tokio::spawn(async move {
                // the relay of the change stream, for each tenant
                loop {
                    let ids: Vec<_> = relay_inst.tenants.read().keys().copied().collect();
                    for t in ids {
                        let s = relay_inst.session(t, Principal::system());
                        let _ = mudcat::stream::relay(&s).await;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            });
            axum::serve(listener, app).await?;
        }
        Cmd::CreateTenant { name, admin } => {
            let t = mudcat::tenants::create_tenant(&inst, &name, &admin).await?;
            println!("{} {} {}", t.id, t.name, t.endpoint);
        }
        Cmd::Tenants => {
            for t in inst.tenants.read().values() {
                println!("{} {} {} {} {}", t.id, t.name, t.endpoint, t.status, t.location);
            }
        }
        Cmd::Token { tenant, principal, hours } => {
            let t = inst.tenant_by_name(&tenant).ok_or_else(|| anyhow::anyhow!("no tenant {tenant}"))?;
            println!("{}", mudcat::auth::issue(&inst, t.id, &Principal(principal), chrono::Duration::hours(hours)));
        }
        Cmd::MoveTenant { tenant, database } => {
            let t = inst.tenant_by_name(&tenant).ok_or_else(|| anyhow::anyhow!("no tenant {tenant}"))?;
            println!("{}", mudcat::tenants::move_tenant(&inst, t.id, &database).await?);
        }
        Cmd::RemoveLeftRows { tenant, database } => {
            let t = inst.tenant_by_name(&tenant).ok_or_else(|| anyhow::anyhow!("no tenant {tenant}"))?;
            println!("{}", mudcat::tenants::remove_left_rows(&inst, t.id, &database, std::time::Duration::from_secs(600)).await?);
        }
        Cmd::AuthzSchema { .. } => unreachable!(),
    }
    Ok(())
}
