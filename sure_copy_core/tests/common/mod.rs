use std::path::PathBuf;
use std::sync::Once;
use std::time::{SystemTime, UNIX_EPOCH};

static INIT_TEST_LOGGING: Once = Once::new();

#[allow(dead_code)]
pub fn init_test_logging() {
    INIT_TEST_LOGGING.call_once(|| {
        let _ = env_logger::Builder::from_env(
            env_logger::Env::default().default_filter_or("sure_copy_core=debug"),
        )
        .is_test(true)
        .try_init();
    });
}

#[allow(dead_code)]
pub fn unique_temp_dir(label: &str) -> PathBuf {
    init_test_logging();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("sure_copy_core_{}_{}", label, nanos))
}

#[allow(dead_code)]
pub fn unique_temp_file(label: &str) -> PathBuf {
    init_test_logging();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("sure_copy_core_{}_{}.txt", label, nanos))
}
