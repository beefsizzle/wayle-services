mod awww;

pub(crate) use awww::{AwwwBackend, spawn_daemon_if_needed};
pub use awww::{
    BezierCurve, Position, TransitionAngle, TransitionConfig, TransitionDuration, TransitionFps,
    TransitionStep, TransitionType, WaveDimensions,
};
