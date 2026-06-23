// SPDX-License-Identifier: AGPL-3.0-or-later

//! `kb-extract`: `Extractor` implementations (text/code, Tika docs, image/VLM, audio/video
//! via ffmpeg+whisper, binary metadata).
//!
//! See plan §2 (per-filetype routing), §7 (pipeline), and §7.1 (video).

pub mod archive;
pub mod audio;
pub mod binary;
pub mod code;
pub mod document;
pub mod email;
pub mod image;
pub mod security;
pub mod text;
pub mod tika;
pub mod video;

#[cfg(test)]
mod test_net;
