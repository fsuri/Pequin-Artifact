SANITIZE_FLAG := -fsanitize=address -static-libasan -fno-omit-frame-pointer -fno-optimize-sibling-calls -fno-sanitize-address-use-after-scope
SANITIZE_FLAG += #-fsanitize=thread  -static-libasan
#add -static-libstdc++ ? doesn't seem to do anything...
#If facing preload bug: export LD_PRELOAD=$(LD_PRELOAD):/usr/lib/gcc/x86_64-linux-gnu/9/libasan.so  (do on cloudlab too)

