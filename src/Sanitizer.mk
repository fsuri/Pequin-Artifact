SANITIZE_FLAG := -fsanitize=address -static-libasan -fno-omit-frame-pointer -fno-optimize-sibling-calls 
SANITIZE_FLAG += #-fsanitize=thread  -static-libtsan
#If facing preload bug: export LD_PRELOAD=$(LD_PRELOAD):/usr/lib/gcc/x86_64-linux-gnu/9/libasan.so  (do on cloudlab too)

