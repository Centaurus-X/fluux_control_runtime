# -*- coding: utf-8 -*-

"""ASCII startup output helpers.

This module is intentionally small, explicit and functional:
- one rendering function
- deterministic ANSI colouring
- no global side effects on import
- no compatibility magic through secondary module homes
"""

import sys
import textwrap


ASCII_TEXT = """
######## ##       ##     ## ##     ##  ######  ##     ## ######## ##     ##
##       ##       ##     ##  ##   ##  ##    ## ##     ## ##       ###   ###
##       ##       ##     ##   ## ##   ##       ##     ## ##       #### ####
######   ##       ##     ##    ###    ##       ######### ######   ## ### ##
##       ##       ##     ##   ## ##   ##       ##     ## ##       ##     ##
##       ##       ##     ##  ##   ##  ##    ## ##     ## ##       ##     ##
##       ########  #######  ##     ##  ######  ##     ## ######## ##     ##
"""

ASCII_CYBER_PUNK = """
================================= CYBER STARTUP =================================
||  deterministic startup  ||  explicit queues  ||  functional orchestration   ||
==================================================================================
"""

ASCII_CTRL = """
           d
          ---
          dt
   +------------------+
-->|   controller      |-----------------------------> y(t)
   +------------------+
            ^
            |
            +-------------------------------
"""

ASCII_PENGUIN = """
   .--.
  |o_o |
  |:_/ |
 //   \\
(|     | )
/\\_   _/\\
\\___)=(___/
"""

METRICS_LEGENDE = """
Queue metrics:
  size      current queue depth
  inflight  open delta between put and get
  put/s     enqueue rate
  get/s     dequeue rate
  err       put/get/timeout counters
"""

CYBER_TEXT_PALETTE = (
    (0, 255, 255),
    (255, 0, 255),
    (0, 200, 255),
    (255, 100, 0),
)

CYBERPALM_PALETTE = {
    "=": (0, 255, 255),
    "|": (255, 0, 255),
    "-": (0, 220, 255),
}


def _safe_write(output_stream, text):
    output_stream.write(str(text))
    output_stream.flush()


def _indent_text_block(text, indent):
    prefix = " " * max(int(indent), 0)
    lines = textwrap.dedent(str(text)).strip("\n").splitlines()
    return "\n".join(prefix + line for line in lines)


def _ansi_rgb_start(rgb_triplet):
    try:
        red = int(rgb_triplet[0])
        green = int(rgb_triplet[1])
        blue = int(rgb_triplet[2])
    except Exception:
        return ""
    return "\033[38;2;%d;%d;%dm" % (red, green, blue)


def _ansi_reset():
    return "\033[0m"


def print_ascii_art(ascii_string, output_stream=sys.stdout, indent=0, rgb=None):
    """Print one ASCII block with optional ANSI colour support.

    Supported colour modes:
    - None: plain text
    - (r, g, b): one colour for the entire block
    - sequence of (r, g, b): palette cycled per character
    - dict mapping char -> (r, g, b): deterministic char palette
    """
    rendered = _indent_text_block(ascii_string, indent)

    if rgb is None:
        _safe_write(output_stream, rendered)
        _safe_write(output_stream, "\n")
        return

    if isinstance(rgb, tuple) and len(rgb) == 3:
        _safe_write(output_stream, _ansi_rgb_start(rgb))
        _safe_write(output_stream, rendered)
        _safe_write(output_stream, _ansi_reset())
        _safe_write(output_stream, "\n")
        return

    if isinstance(rgb, dict):
        for line in rendered.splitlines():
            for char in line:
                colour = rgb.get(char)
                if colour is None:
                    _safe_write(output_stream, char)
                else:
                    _safe_write(output_stream, _ansi_rgb_start(colour))
                    _safe_write(output_stream, char)
                    _safe_write(output_stream, _ansi_reset())
            _safe_write(output_stream, "\n")
        return

    if isinstance(rgb, (list, tuple)) and rgb:
        palette = []
        for colour in rgb:
            if isinstance(colour, tuple) and len(colour) == 3:
                palette.append(colour)

        if palette:
            palette_len = len(palette)
            palette_index = 0
            for line in rendered.splitlines():
                for char in line:
                    if char.isspace():
                        _safe_write(output_stream, char)
                        continue
                    colour = palette[palette_index % palette_len]
                    palette_index += 1
                    _safe_write(output_stream, _ansi_rgb_start(colour))
                    _safe_write(output_stream, char)
                    _safe_write(output_stream, _ansi_reset())
                _safe_write(output_stream, "\n")
            return

    _safe_write(output_stream, rendered)
    _safe_write(output_stream, "\n")


__all__ = (
    "ASCII_TEXT",
    "ASCII_CYBER_PUNK",
    "ASCII_CTRL",
    "ASCII_PENGUIN",
    "METRICS_LEGENDE",
    "CYBER_TEXT_PALETTE",
    "CYBERPALM_PALETTE",
    "print_ascii_art",
)
